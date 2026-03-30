import logging
import multiprocessing
import queue
import random
import signal as _signal_mod
import threading
import time
import uuid
from concurrent.futures import ProcessPoolExecutor, as_completed, FIRST_COMPLETED, wait
from datetime import datetime
from pathlib import Path
from collections import deque
from typing import List, Dict, Optional, Tuple

import requests
import typer
from tqdm import tqdm

from genrobot.exit_codes import EXIT_SUCCESS, EXIT_PARTIAL_FAILURE, EXIT_USER_CANCELLED
from genrobot.i18n import t

logger = logging.getLogger('genrobot')

BATCH_SIZE = 200
DOWNLOAD_CHUNK_SIZE = 1024 * 128  # 128KiB


class URLExpiredError(Exception):
    """URL 过期异常（S3/TOS 返回 403/404），触发批量重新签发"""
    pass


class _ShutdownRequested(Exception):
    """用户请求停止下载（Ctrl+C），用于协作式关闭流程"""
    pass


# ======================================================================
# Worker process: top-level functions + shared state via initializer
# ======================================================================

_wp_shared_bytes: Optional[multiprocessing.Value] = None
_wp_shutdown: Optional[multiprocessing.Event] = None
_wp_run_id: Optional[str] = None


def _worker_init(shared_bytes, shutdown_event, run_id):
    """Worker process initializer — sets shared state and ignores SIGINT."""
    global _wp_shared_bytes, _wp_shutdown, _wp_run_id
    _wp_shared_bytes = shared_bytes
    _wp_shutdown = shutdown_event
    _wp_run_id = run_id
    # Let the main process handle Ctrl+C; workers just check the event
    _signal_mod.signal(_signal_mod.SIGINT, _signal_mod.SIG_IGN)


def _is_retryable_worker(exc: Exception) -> bool:
    """Worker-safe retryable check (avoids cross-module circular imports)."""
    if isinstance(exc, URLExpiredError):
        return False
    if isinstance(exc, requests.exceptions.HTTPError):
        if exc.response is not None:
            return exc.response.status_code not in {401, 403}
        return True
    return isinstance(exc, (
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        ValueError,
    ))


def _download_file_once_wp(url: str, local_path: Path, expected_size: int) -> None:
    """Download a single file (single attempt) inside a worker process."""
    part_file = local_path.with_suffix(local_path.suffix + '.part')
    if part_file.exists():
        part_file.unlink()

    try:
        resp = requests.get(url, stream=True, timeout=120)
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code in (403, 404):
            raise URLExpiredError(
                f'URL expired or invalid: {e.response.status_code}'
            )
        raise

    with open(part_file, 'wb') as f:
        for chunk in resp.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
            if _wp_shutdown.is_set():
                raise _ShutdownRequested()
            f.write(chunk)
            with _wp_shared_bytes.get_lock():
                _wp_shared_bytes.value += len(chunk)

    actual_size = part_file.stat().st_size
    if expected_size > 0 and actual_size != expected_size:
        part_file.unlink()
        raise ValueError(
            f'Size mismatch: expected {expected_size}, got {actual_size}'
        )

    part_file.rename(local_path)


def _download_file_wp(args: Tuple[str, str, int]) -> Tuple:
    """Top-level worker entry point. Returns a result tuple.

    Result forms:
        ('success',     path_str, size)
        ('cancelled',   path_str)
        ('url_expired', path_str)
        ('failed',      path_str, error_msg)
    """
    url, local_path_str, expected_size = args
    local_path = Path(local_path_str)

    max_retries = 3
    initial_delay = 1.0
    backoff_factor = 2.0
    max_delay = 32.0
    delay = initial_delay

    for attempt in range(max_retries):
        try:
            _download_file_once_wp(url, local_path, expected_size)
            return ('success', local_path_str, expected_size)
        except _ShutdownRequested:
            return ('cancelled', local_path_str)
        except URLExpiredError:
            return ('url_expired', local_path_str)
        except Exception as e:
            if attempt == max_retries - 1 or not _is_retryable_worker(e):
                return ('failed', local_path_str, str(e))

            jitter = delay * 0.25 * (random.random() * 2 - 1)
            sleep_time = min(delay + jitter, max_delay)
            delay = min(delay * backoff_factor, max_delay)

            # Use Event.wait so a shutdown signal interrupts the sleep
            if _wp_shutdown.wait(timeout=sleep_time):
                return ('cancelled', local_path_str)

    return ('failed', local_path_str, 'max retries exceeded')


# ======================================================================
# Formatting helpers
# ======================================================================

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


# ======================================================================
# Progress bar (main process only — polls shared_bytes from workers)
# ======================================================================

class _DownloadProgress:
    """封装 tqdm 进度条，支持文件数和字节数双维度更新，实时速度统计。

    In multi-process mode a background thread polls the shared byte counter
    written to by worker processes and updates the display.
    """

    _SPEED_WINDOW = 2.0
    _SAMPLE_INTERVAL = 0.2

    def __init__(self, total_files: int, total_size: int,
                 shared_bytes: Optional[multiprocessing.Value] = None):
        self.total_files = total_files
        self.total_size = total_size
        self.downloaded_bytes = 0
        self._shared_bytes = shared_bytes
        self._lock = threading.Lock()
        self._start_time = time.monotonic()
        self._speed_samples: deque = deque()
        self._last_sample_time = 0.0
        self._stop_polling = threading.Event()
        self._poll_thread: Optional[threading.Thread] = None
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

        # Start a polling thread when using a shared byte counter
        if shared_bytes is not None:
            self._poll_thread = threading.Thread(
                target=self._poll_loop, daemon=True, name='progress-poll',
            )
            self._poll_thread.start()

    # ---- polling (multi-process mode) ----

    def _poll_loop(self) -> None:
        while not self._stop_polling.wait(timeout=self._SAMPLE_INTERVAL):
            self._sync_shared_bytes()

    def _sync_shared_bytes(self) -> None:
        with self._shared_bytes.get_lock():
            current = self._shared_bytes.value
        with self._lock:
            self.downloaded_bytes = current
            now = time.monotonic()
            self._speed_samples.append((now, current))
            cutoff = now - self._SPEED_WINDOW
            while self._speed_samples and self._speed_samples[0][0] < cutoff:
                self._speed_samples.popleft()
        self._refresh_desc()
        self._bar.refresh()

    # ---- public API (called from main process) ----

    def update(self, files: int = 1) -> None:
        """推进文件计数"""
        if self._shared_bytes is not None:
            self._sync_shared_bytes()
        self._refresh_desc()
        self._bar.update(files)

    def _calc_recent_speed(self) -> float:
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
        self._stop_polling.set()
        if self._poll_thread:
            self._poll_thread.join(timeout=1)
        if self._shared_bytes is not None:
            self._sync_shared_bytes()
        self._bar.close()

    @property
    def total_downloaded(self) -> int:
        if self._shared_bytes is not None:
            with self._shared_bytes.get_lock():
                return self._shared_bytes.value
        return self.downloaded_bytes


# ======================================================================
# DownloadService
# ======================================================================

class DownloadService:
    def __init__(self, client, concurrency: int = 4):
        self.client = client
        self.concurrency = concurrency
        self.run_id = str(uuid.uuid4())
        # multiprocessing.Event so worker processes can observe shutdown
        self._shutdown = multiprocessing.Event()
        # Shared byte counter written by workers, read by progress poller
        self._shared_bytes = multiprocessing.Value('q', 0)  # signed long long

    def request_shutdown(self) -> None:
        """请求优雅停止（由信号处理器调用）"""
        self._shutdown.set()

    @property
    def is_shutting_down(self) -> bool:
        return self._shutdown.is_set()

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
    # 结果收集
    # ------------------------------------------------------------------

    def _collect_result(
        self, result: Tuple, item: Dict, progress: _DownloadProgress,
        url_expired_items: List[Dict],
        counters: Dict,
    ) -> None:
        """处理 worker 返回的结果 tuple，更新计数器和进度条"""
        status = result[0]
        if status == 'success':
            counters['succeeded'] += 1
            counters['downloaded_bytes'] += item['size']
            progress.update(1)
        elif status == 'cancelled':
            logger.info(
                f'[{self.run_id}] file_cancelled file={item["local_path"].name}'
            )
        elif status == 'url_expired':
            url_expired_items.append(item)
            logger.warning(
                f'[{self.run_id}] url_expired file={item["local_path"].name}'
            )
        elif status == 'failed':
            counters['failed'] += 1
            progress.update(1)
            error_msg = result[2] if len(result) > 2 else 'unknown'
            logger.error(
                f'[{self.run_id}] file_download_failed '
                f'file={item["local_path"].name} error={error_msg}'
            )

    # ------------------------------------------------------------------
    # 主入口：流水线式多进程下载
    # ------------------------------------------------------------------

    def _start_producer(
        self, dataset_token: str, output_dir: Path,
        organize_by_sst: bool, skip_existing: bool,
        task_queue: queue.Queue,
    ) -> threading.Thread:
        """启动后台生产者线程：拉取 meta → 过滤 → 签发 URL → 放入队列。

        队列消息格式:
            ('task',   url, item)  — 待下载文件
            ('skip',   count)      — 已跳过的文件数
            ('no_url', item)       — URL 签发失败
            None                   — 生产者结束（哨兵）
        """
        def _produce():
            cursor = None
            page_num = 0
            try:
                while True:
                    if self.is_shutting_down:
                        return

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

                    tasks, skipped = self._filter_and_build_tasks(
                        page_metas, dataset_token, output_dir,
                        organize_by_sst, skip_existing,
                    )
                    if skipped:
                        self._queue_put(task_queue, ('skip', skipped))

                    if tasks:
                        specs = [item['download_spec'] for item in tasks]
                        url_map = self._issue_urls(specs, dataset_token)

                        for item in tasks:
                            if self.is_shutting_down:
                                return
                            url = url_map.get(item['download_spec'])
                            if url:
                                self._queue_put(task_queue, ('task', url, item))
                            else:
                                self._queue_put(task_queue, ('no_url', item))

                    if not data.get('has_more'):
                        return
                    cursor = data.get('cursor')
            except Exception as e:
                logger.error(
                    f'[{self.run_id}] producer_error error={e}'
                )
            finally:
                task_queue.put(None)  # sentinel — always sent

        thread = threading.Thread(target=_produce, daemon=True, name='task-producer')
        thread.start()
        return thread

    def _queue_put(self, q: queue.Queue, item) -> None:
        """向队列放入元素，支持 shutdown 中断（避免 put 永久阻塞）"""
        while not self.is_shutting_down:
            try:
                q.put(item, timeout=0.5)
                return
            except queue.Full:
                continue

    def download_dataset(
        self, dataset_token: str, output_dir: Path,
        organize_by_sst: bool = True, skip_existing: bool = True,
    ) -> int:
        """下载数据集（后台生产者 + 多进程流水线），返回退出码"""
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

        typer.echo(t('dataset_info', dataset_name))
        typer.echo(t('dataset_file_size', total_files, _format_size(total_size)))
        typer.echo('')
        typer.echo(t('download_start', self.concurrency))

        # 2. 启动后台生产者 + 多进程消费者流水线
        total_skipped = 0
        counters: Dict = {'succeeded': 0, 'failed': 0, 'downloaded_bytes': 0}

        progress = _DownloadProgress(
            total_files, total_size, self._shared_bytes,
        )

        # 有界队列：生产者放入任务，主线程取出并提交给进程池
        # 容量设为 concurrency×3，保证 workers 永远有活干
        task_queue: queue.Queue = queue.Queue(maxsize=self.concurrency * 3)
        producer_thread = self._start_producer(
            dataset_token, output_dir, organize_by_sst, skip_existing,
            task_queue,
        )

        with ProcessPoolExecutor(
            max_workers=self.concurrency,
            initializer=_worker_init,
            initargs=(self._shared_bytes, self._shutdown, self.run_id),
        ) as executor:
            pending: Dict = {}          # future -> item
            url_expired_items: List[Dict] = []
            producer_done = False

            # ---- 阶段 1：交替提交任务和收割结果 ----
            while not producer_done:
                if self.is_shutting_down:
                    break

                # 1a. 收割已完成的 future
                done_futures = [f for f in pending if f.done()]
                for future in done_futures:
                    item = pending.pop(future)
                    self._collect_result(
                        future.result(), item, progress,
                        url_expired_items, counters,
                    )

                # 1b. 批量拉取队列中所有就绪任务并提交
                pulled = False
                while True:
                    try:
                        msg = task_queue.get_nowait()
                        pulled = True
                    except queue.Empty:
                        break

                    if msg is None:
                        producer_done = True
                        break
                    elif msg[0] == 'skip':
                        total_skipped += msg[1]
                        progress.update(msg[1])
                    elif msg[0] == 'no_url':
                        counters['failed'] += 1
                        progress.update(1)
                    elif msg[0] == 'task':
                        _, url, item = msg
                        work = (url, str(item['local_path']), item['size'])
                        future = executor.submit(_download_file_wp, work)
                        pending[future] = item

                # 1c. 队列空且生产者未结束 → 短暂等待避免空转
                if not pulled and not producer_done:
                    try:
                        msg = task_queue.get(timeout=0.05)
                    except queue.Empty:
                        continue

                    if msg is None:
                        producer_done = True
                    elif msg[0] == 'skip':
                        total_skipped += msg[1]
                        progress.update(msg[1])
                    elif msg[0] == 'no_url':
                        counters['failed'] += 1
                        progress.update(1)
                    elif msg[0] == 'task':
                        _, url, item = msg
                        work = (url, str(item['local_path']), item['size'])
                        future = executor.submit(_download_file_wp, work)
                        pending[future] = item

            # ---- 阶段 2：等待剩余在途下载完成 ----
            for future in as_completed(list(pending)):
                if self.is_shutting_down:
                    break
                item = pending.pop(future)
                self._collect_result(
                    future.result(), item, progress,
                    url_expired_items, counters,
                )

            # ---- 阶段 3：URL 过期重试 ----
            if url_expired_items and not self.is_shutting_down:
                logger.info(
                    f'[{self.run_id}] url_expired_reissue '
                    f'count={len(url_expired_items)}'
                )
                retry_specs = [item['download_spec'] for item in url_expired_items]
                new_url_map = self._issue_urls(retry_specs, dataset_token)

                retry_futures: Dict = {}
                for item in url_expired_items:
                    if self.is_shutting_down:
                        break
                    url = new_url_map.get(item['download_spec'])
                    if not url:
                        counters['failed'] += 1
                        progress.update(1)
                        continue
                    work = (url, str(item['local_path']), item['size'])
                    future = executor.submit(_download_file_wp, work)
                    retry_futures[future] = item

                for future in as_completed(retry_futures):
                    item = retry_futures[future]
                    result = future.result()
                    status = result[0]
                    if status == 'success':
                        counters['succeeded'] += 1
                        counters['downloaded_bytes'] += item['size']
                        progress.update(1)
                    elif status == 'cancelled':
                        logger.info(
                            f'[{self.run_id}] file_cancelled '
                            f'file={item["local_path"].name}'
                        )
                    else:
                        counters['failed'] += 1
                        progress.update(1)
                        error_msg = result[2] if len(result) > 2 else 'unknown'
                        logger.error(
                            f'[{self.run_id}] file_download_failed_after_reissue '
                            f'file={item["local_path"].name} error={error_msg}'
                        )

        producer_thread.join(timeout=2)
        progress.close()

        # 3. 输出摘要
        total_succeeded = counters['succeeded']
        total_failed = counters['failed']
        total_downloaded_bytes = counters['downloaded_bytes']
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
