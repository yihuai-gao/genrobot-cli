import logging
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from collections import deque
from typing import Callable, List, Dict, Optional, Tuple

import requests
import typer
from tqdm import tqdm

from genrobot.exit_codes import EXIT_SUCCESS, EXIT_PARTIAL_FAILURE, EXIT_USER_CANCELLED
from genrobot.i18n import t
from genrobot.utils import retry_on_error

logger = logging.getLogger('genrobot')

BATCH_SIZE = 20
DOWNLOAD_CHUNK_SIZE = 1024 * 128  # 128KiB


class URLExpiredError(Exception):
    """URL 过期异常（S3/TOS 返回 403/404），触发批量重新签发"""
    pass


class _ShutdownRequested(Exception):
    """用户请求停止下载（Ctrl+C），用于协作式关闭流程"""
    pass


def _format_size(size_bytes: int, width: int = 0) -> str:
    if size_bytes >= 1024 ** 4:
        s = f'{size_bytes / 1024 ** 4:.2f} TB'
    elif size_bytes >= 1024 ** 3:
        s = f'{size_bytes / 1024 ** 3:.2f} GB'
    elif size_bytes >= 1024 ** 2:
        s = f'{size_bytes / 1024 ** 2:.1f} MB'
    else:
        s = f'{size_bytes / 1024:.0f} KB'
    return s.rjust(width) if width else s


def _format_elapsed(seconds: float, width: int = 0) -> str:
    """将秒数格式化为人类友好的耗时字符串"""
    seconds = max(0, int(seconds))
    if seconds < 60:
        s = t('elapsed_seconds', seconds)
    elif seconds < 3600:
        m, sec = divmod(seconds, 60)
        s = t('elapsed_minutes', m, sec)
    else:
        h, remainder = divmod(seconds, 3600)
        m = remainder // 60
        s = t('elapsed_hours', h, m)
    return s.rjust(width) if width else s


class _DownloadProgress:
    """封装 tqdm 进度条，支持文件数和字节数双维度更新，实时速度统计"""

    # 滑动窗口大小（秒），用于计算实时速度
    _SPEED_WINDOW = 2.0
    # 采样间隔（秒），控制滑动窗口采样频率
    _SAMPLE_INTERVAL = 0.2

    def __init__(self, total_files: int, total_size: int):
        self.total_files = total_files
        self.total_size = total_size
        self.downloaded_bytes = 0
        self._lock = threading.Lock()
        self._start_time = time.monotonic()
        # 滑动窗口采样：(timestamp, cumulative_bytes)
        self._speed_samples: deque = deque()
        self._last_sample_time = 0.0
        self._bar = tqdm(
            total=total_files,
            bar_format=(
                '{percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} '
                + t('files_unit')
                + ' | {desc}'
            ),
            leave=True,
        )
        self._refresh_desc()
        self._bar.refresh()

    def update(self, files: int = 1) -> None:
        """推进文件计数（字节数通过 update_bytes 实时累加，此处不再重复加）"""
        self._refresh_desc()
        self._bar.update(files)

    def update_bytes(self, bytes_delta: int) -> None:
        """实时更新已下载字节数（chunk 级别调用），不推进文件计数"""
        should_refresh = False
        with self._lock:
            self.downloaded_bytes += bytes_delta
            now = time.monotonic()
            if now - self._last_sample_time >= self._SAMPLE_INTERVAL:
                self._speed_samples.append((now, self.downloaded_bytes))
                self._last_sample_time = now
                # 清理超出窗口的旧样本
                cutoff = now - self._SPEED_WINDOW
                while self._speed_samples and self._speed_samples[0][0] < cutoff:
                    self._speed_samples.popleft()
                should_refresh = True
        # 仅在采样间隔到达时刷新显示，避免过于频繁的终端刷新
        if should_refresh:
            self._refresh_desc()
            self._bar.refresh()

    def rollback_bytes(self, amount: int) -> None:
        """回退已累加的字节数（文件下载失败时调用）"""
        with self._lock:
            self.downloaded_bytes = max(0, self.downloaded_bytes - amount)

    def _calc_recent_speed(self) -> float:
        """基于滑动窗口计算实时速度 (bytes/s)"""
        if not self._speed_samples:
            return 0.0
        now = time.monotonic()
        oldest_time, oldest_bytes = self._speed_samples[0]
        dt = now - oldest_time
        if dt < 0.1:
            return 0.0
        return (self.downloaded_bytes - oldest_bytes) / dt

    def _refresh_desc(self) -> None:
        elapsed = time.monotonic() - self._start_time
        avg_speed = self.downloaded_bytes / elapsed if elapsed > 0 else 0
        cur_speed = self._calc_recent_speed()
        remaining_bytes = max(0, self.total_size - self.downloaded_bytes)
        # ETA 优先使用实时速度（更准确），回退到平均速度
        effective_speed = cur_speed if cur_speed > 0 else avg_speed
        eta_str = _format_elapsed(remaining_bytes / effective_speed, width=7) if effective_speed > 0 else '     --'

        avg_label = t('avg_speed_label')
        cur_label = t('cur_speed_label')
        desc = (
            f'{_format_size(self.downloaded_bytes, 9)}/{_format_size(self.total_size, 9)}'
            f' | {avg_label}: {avg_speed / 1024 / 1024:7.1f} MB/s'
            f' | {cur_label}: {cur_speed / 1024 / 1024:7.1f} MB/s'
            f' | {t("eta_label")}: {eta_str}'
        )
        self._bar.set_description(desc, refresh=False)

    def close(self) -> None:
        self._bar.close()

    @property
    def total_downloaded(self) -> int:
        return self.downloaded_bytes


class DownloadService:
    def __init__(self, client, concurrency: int = 4):
        self.client = client
        self.concurrency = concurrency
        self.run_id = str(uuid.uuid4())
        # 协作式关闭：设置后所有工作线程尽快停止
        self._shutdown = threading.Event()

    def request_shutdown(self) -> None:
        """请求优雅停止（由信号处理器调用）"""
        self._shutdown.set()

    @property
    def is_shutting_down(self) -> bool:
        return self._shutdown.is_set()

    # ------------------------------------------------------------------
    # 单文件下载
    # ------------------------------------------------------------------

    def _download_file_once(
        self, url: str, local_path: Path, expected_size: int,
        on_chunk: Optional[Callable[[int], None]] = None,
    ) -> None:
        """下载单个文件（单次尝试）"""
        part_file = local_path.with_suffix(local_path.suffix + '.part')

        # MVP 简化：残留 .part 直接删除后重下
        if part_file.exists():
            part_file.unlink()

        logger.debug(f'[{self.run_id}] file_download_started file={local_path.name}')

        try:
            # url = url.replace(".mcap", "_vio.mcap")
            # url = url.split(".mcap")[0] + "_vio.mcap?response-content-type=application%2Foctet-stream&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIA22SRFQLKY6AY4EPW%2F20260329%2Fcn-northwest-1%2Fs3%2Faws4_request&X-Amz-Date=20260329T234822Z&X-Amz-Expires=7200&X-Amz-SignedHeaders=host&X-Amz-Signature=d9f224535f32dd17f1297ed8c520aa5bd3a1eaa0564e6caa2dfed8949c95dd13"
            # https://data-whale.s3.cn-northwest-1.amazonaws.com.cn/DAS-G-rv1126b-2ef41c0ef15d6c87-edge_events-das-gripper_20260325033408_none_none_00000000_vio.mcap?response-content-type=application%2Foctet-stream&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIA22SRFQLKY6AY4EPW%2F20260329%2Fcn-northwest-1%2Fs3%2Faws4_request&X-Amz-Date=20260329T233328Z&X-Amz-Expires=7200&X-Amz-SignedHeaders=host&X-Amz-Signature=3115afdf2e9821d61d285c7c1e242b01a3c66a1984aea9676944599c29daa7d6
            print(url)
            resp = requests.get(url, stream=True, timeout=120)
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            # S3/TOS URL 过期返回 403 或 404
            if e.response is not None and e.response.status_code in (403, 404):
                raise URLExpiredError(
                    f'URL expired or invalid: {e.response.status_code}'
                )
            raise

        with open(part_file, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                # 每个 chunk 检查关闭信号，保证最大 128KiB 粒度的响应延迟
                if self.is_shutting_down:
                    raise _ShutdownRequested()
                f.write(chunk)
                if on_chunk:
                    on_chunk(len(chunk))

        # 校验大小
        actual_size = part_file.stat().st_size
        if expected_size > 0 and actual_size != expected_size:
            part_file.unlink()
            raise ValueError(
                f'Size mismatch: expected {expected_size}, got {actual_size}'
            )

        # 原子 rename
        part_file.rename(local_path)
        logger.debug(f'[{self.run_id}] file_download_succeeded file={local_path.name}')

    def _download_file(
        self, url: str, local_path: Path, expected_size: int,
        on_chunk: Optional[Callable[[int], None]] = None,
    ) -> None:
        """下载单个文件（带重试，不重试 URLExpiredError 和 _ShutdownRequested）

        重试间隔使用 shutdown 事件等待代替 time.sleep，保证 Ctrl+C 时等待可被立即中断。
        """
        from genrobot.utils import is_retryable_error
        from genrobot.http_client import RateLimitedError

        max_retries = 3
        initial_delay = 1.0
        backoff_factor = 2.0
        max_delay = 32.0
        delay = initial_delay

        for attempt in range(max_retries):
            try:
                self._download_file_once(url, local_path, expected_size, on_chunk)
                return
            except _ShutdownRequested:
                # 关闭请求不重试，直接向上传播
                raise
            except Exception as e:
                if attempt == max_retries - 1 or not is_retryable_error(e):
                    raise

                # 429 使用服务端指定的等待时间
                if isinstance(e, RateLimitedError):
                    sleep_time = e.retry_after
                else:
                    import random
                    jitter = delay * 0.25 * (random.random() * 2 - 1)
                    sleep_time = min(delay + jitter, max_delay)
                    delay = min(delay * backoff_factor, max_delay)

                # 用 Event.wait 代替 time.sleep，使等待可被 shutdown 信号立即中断
                interrupted = self._shutdown.wait(timeout=sleep_time)
                if interrupted:
                    raise _ShutdownRequested()

    # ------------------------------------------------------------------
    # 本地去重 & 任务构建
    # ------------------------------------------------------------------

    @staticmethod
    def _build_local_path(output_dir: Path, meta: dict, organize_by_sst: bool) -> Path:
        sst = meta.get('sst', {})
        if organize_by_sst:
            return (
                output_dir
                / sst.get('domain', 'unknown_domain')
                / sst.get('scenario', 'unknown_scenario')
                / sst.get('task', 'unknown_task')
                / sst.get('skill', 'unknown_skill')
                / meta['filename']
            )
        return output_dir / meta['filename']

    def _filter_and_build_tasks(
        self, metas: list, dataset_token: str,
        output_dir: Path, organize_by_sst: bool, skip_existing: bool,
    ) -> Tuple[List[Dict], int]:
        """对一批 meta 构建下载任务列表，跳过已完成文件"""
        tasks: List[Dict] = []
        skipped = 0

        for meta in metas:
            local_path = self._build_local_path(
                output_dir, meta, organize_by_sst
            )

            if skip_existing and local_path.exists():
                expected = meta.get('size', 0)
                if expected <= 0 or local_path.stat().st_size == expected:
                    logger.debug(
                        f'[{self.run_id}] file_skipped_existing file={local_path.name}'
                    )
                    skipped += 1
                    continue

            local_path.parent.mkdir(parents=True, exist_ok=True)
            tasks.append({
                'download_spec': meta['download_spec'],
                'local_path': local_path,
                'size': meta.get('size', 0),
            })

        return tasks, skipped

    # ------------------------------------------------------------------
    # URL 签发
    # ------------------------------------------------------------------

    def _issue_urls(self, specs: List[str], dataset_token: str) -> Dict[str, str]:
        """批量签发下载 URL，返回 {download_spec: url}"""
        resp = self.client.post(
            f'/partner/datasets/{dataset_token}/download-urls:batch-issue',
            json={'download_specs': specs},
        )
        urls_list = resp.get('data', {}).get('urls', [])
        return {entry['download_spec']: entry['url'] for entry in urls_list}

    # ------------------------------------------------------------------
    # 批次执行
    # ------------------------------------------------------------------

    def _execute_batch(
        self, batch: List[Dict], dataset_token: str,
        progress: _DownloadProgress,
    ) -> Tuple[int, int, int]:
        """执行单批次：签发 URL → 并发下载。返回 (succeeded, failed, downloaded_bytes)"""
        specs = [item['download_spec'] for item in batch]
        url_map = self._issue_urls(specs, dataset_token)

        succeeded = 0
        failed = 0
        downloaded_bytes = 0
        url_expired_items: List[Dict] = []

        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            futures = {}
            for item in batch:
                # 关闭信号已设置时不再提交新任务
                if self.is_shutting_down:
                    break
                url = url_map.get(item['download_spec'])
                if not url:
                    failed += 1
                    progress.update(1)
                    continue
                future = executor.submit(
                    self._download_file, url, item['local_path'], item['size'],
                    on_chunk=progress.update_bytes,
                )
                futures[future] = item

            for future in as_completed(futures):
                item = futures[future]
                try:
                    future.result()
                    succeeded += 1
                    downloaded_bytes += item['size']
                    # 字节数已在 chunk 回调中实时累加，此处只推进文件计数
                    progress.update(1)
                except _ShutdownRequested:
                    # 用户取消：不计入失败，不更新进度（留给 summary 显示）
                    logger.info(
                        f'[{self.run_id}] file_cancelled file={item["local_path"].name}'
                    )
                except URLExpiredError:
                    url_expired_items.append(item)
                    logger.warning(
                        f'[{self.run_id}] url_expired file={item["local_path"].name}'
                    )
                except Exception as e:
                    failed += 1
                    progress.update(1)
                    logger.error(
                        f'[{self.run_id}] file_download_failed '
                        f'file={item["local_path"].name} error={e}'
                    )

        # 已请求关闭则跳过 URL 过期重试，让主循环尽快退出
        if url_expired_items and not self.is_shutting_down:
            logger.info(
                f'[{self.run_id}] url_expired_reissue count={len(url_expired_items)}'
            )
            retry_specs = [item['download_spec'] for item in url_expired_items]
            new_url_map = self._issue_urls(retry_specs, dataset_token)

            with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
                futures = {}
                for item in url_expired_items:
                    if self.is_shutting_down:
                        break
                    url = new_url_map.get(item['download_spec'])
                    if not url:
                        failed += 1
                        progress.update(1)
                        continue
                    future = executor.submit(
                        self._download_file, url, item['local_path'], item['size'],
                        on_chunk=progress.update_bytes,
                    )
                    futures[future] = item

                for future in as_completed(futures):
                    item = futures[future]
                    try:
                        future.result()
                        succeeded += 1
                        downloaded_bytes += item['size']
                        progress.update(1)
                    except _ShutdownRequested:
                        logger.info(
                            f'[{self.run_id}] file_cancelled file={item["local_path"].name}'
                        )
                    except Exception as e:
                        failed += 1
                        progress.update(1)
                        logger.error(
                            f'[{self.run_id}] file_download_failed_after_reissue '
                            f'file={item["local_path"].name} error={e}'
                        )

        return succeeded, failed, downloaded_bytes

    # ------------------------------------------------------------------
    # 主入口：流式分批下载
    # ------------------------------------------------------------------

    def download_dataset(
        self, dataset_token: str, output_dir: Path,
        organize_by_sst: bool = True, skip_existing: bool = True,
    ) -> int:
        """下载数据集（流式分批模式），返回退出码"""
        start_time = datetime.now()

        # 1. 获取数据集统计信息
        typer.echo(t('fetching_metadata'))

        stats_resp = self.client.get(f'/partner/datasets/{dataset_token}')
        stats_code = stats_resp.get('code')
        if stats_code not in (0, '0', '0000'):
            error_code = stats_resp.get('data', {}).get('error_code', '')
            if error_code in ('NOT_SUBSCRIBED', 'SUBSCRIPTION_INACTIVE', 'SUBSCRIPTION_EXPIRED'):
                typer.echo(t('not_subscribed'))
            else:
                typer.echo(f'✗ {stats_resp.get("message", "Unknown error")}')
            from genrobot.exit_codes import EXIT_NOT_SUBSCRIBED
            return EXIT_NOT_SUBSCRIBED

        stats_data = stats_resp.get('data', {})
        ds = stats_data.get('dataset', stats_data)
        total_files = int(ds.get('data_counts', 0))
        total_size = int(ds.get('total_size', 0))
        dataset_name = ds.get('name', dataset_token)

        logger.info(
            f'[{self.run_id}] dataset_stats dataset={dataset_token} '
            f'total_files={total_files} total_size={total_size}'
        )

        # 显示数据集信息（对齐 PRD 8.4）
        typer.echo(t('dataset_info', dataset_name))
        typer.echo(t('dataset_file_size', total_files, _format_size(total_size)))
        typer.echo('')
        typer.echo(t('download_start', self.concurrency))

        # 2. 流式分批：逐页拉取 meta → 过滤 → 签发 → 下载
        total_succeeded = 0
        total_skipped = 0
        total_failed = 0
        total_downloaded_bytes = 0
        cursor = None
        page_num = 0

        progress = _DownloadProgress(total_files, total_size)

        while True:
            # 用户已请求关闭，停止拉取新批次
            if self.is_shutting_down:
                break

            # 2a. 拉取一页 meta
            params: Dict = {'limit': BATCH_SIZE}
            if cursor:
                params['cursor'] = cursor

            resp = self.client.get(
                f'/partner/datasets/{dataset_token}/download-meta',
                params=params,
            )
            data = resp.get('data', {})
            page_metas = data.get('items', [])
            page_num += 1

            logger.info(
                f'[{self.run_id}] meta_page_fetched page={page_num} '
                f'items={len(page_metas)}'
            )

            # 2b. 过滤已完成文件，构建下载任务
            tasks, skipped = self._filter_and_build_tasks(
                page_metas, dataset_token, output_dir,
                organize_by_sst, skip_existing,
            )
            total_skipped += skipped
            progress.update(skipped)

            # 2c. 签发 URL 并下载
            if tasks:
                succeeded, failed, batch_bytes = self._execute_batch(
                    tasks, dataset_token, progress,
                )
                total_succeeded += succeeded
                total_failed += failed
                total_downloaded_bytes += batch_bytes

            # 2d. 检查是否还有更多页
            if not data.get('has_more'):
                break
            cursor = data.get('cursor')

        progress.close()

        # 3. 输出摘要（对齐 PRD 8.4 格式）
        cancelled = self.is_shutting_down
        elapsed = (datetime.now() - start_time).total_seconds()
        avg_speed = (
            total_downloaded_bytes / elapsed / 1024 / 1024
            if elapsed > 0 else 0
        )

        typer.echo('')
        if cancelled:
            typer.echo(t('download_cancelled'))
        elif total_failed == 0:
            typer.echo(t('download_complete'))
        else:
            typer.echo(t('download_partial_fail'))

        size_str = _format_size(total_downloaded_bytes)
        typer.echo('')
        typer.echo(t('stats_header'))
        typer.echo(f'  {t("success_summary", total_succeeded, size_str)}')
        typer.echo(f'  {t("skipped_summary", total_skipped)}')
        typer.echo(f'  {t("failed_summary", total_failed)}')
        typer.echo('')
        typer.echo(t('total_time', _format_elapsed(elapsed)))
        typer.echo(t('avg_speed', avg_speed))
        typer.echo(t('run_id_label', self.run_id))

        logger.info(
            f'[{self.run_id}] run_completed '
            f'downloaded={total_succeeded} skipped={total_skipped} '
            f'failed={total_failed} bytes={total_downloaded_bytes} '
            f'elapsed={elapsed:.1f}s cancelled={cancelled}'
        )

        if cancelled:
            return EXIT_USER_CANCELLED
        return EXIT_PARTIAL_FAILURE if total_failed > 0 else EXIT_SUCCESS
