import gc
import os
import signal
import sys
import threading
import time
import typer
from pathlib import Path

from genrobot.config import load_config
from genrobot.download_service import DownloadService
from genrobot.exit_codes import EXIT_TOKEN_INVALID, EXIT_INVALID_ARGS
from genrobot.http_client import APIClient
from genrobot.i18n import t
from genrobot.logger import setup_logger


def _release_tqdm_mp_lock() -> None:
    """释放 tqdm 持有的 multiprocessing.RLock，避免 os._exit 产生 POSIX semaphore 泄漏警告。

    tqdm 的 TqdmDefaultWriteLock 首次使用时会创建一个 multiprocessing.RLock()，
    该对象在 macOS/Linux 上对应一个 POSIX named semaphore，由 multiprocessing.resource_tracker
    子进程追踪。os._exit() 绕过 Python 的 atexit 清理，导致 RLock.__del__（会调用 sem_unlink）
    永远不会执行，resource_tracker 因此报告泄漏。

    解决方式：置空类级别引用 → gc.collect() 触发 __del__ → sem_unlink 完成清理。
    """
    try:
        from tqdm import tqdm as _tqdm_cls
        from tqdm.std import TqdmDefaultWriteLock
        for bar in list(_tqdm_cls._instances):
            try:
                bar.close()
            except Exception:
                pass
        if hasattr(TqdmDefaultWriteLock, 'mp_lock'):
            TqdmDefaultWriteLock.mp_lock = None
        gc.collect()
    except Exception:
        pass


def _install_sigint_handler(service: DownloadService) -> signal.Handlers:
    """安装两级 Ctrl+C 处理器，返回原始 handler 以便事后还原。

    第一次 Ctrl+C：请求优雅停止，启动 5 秒看门狗强制兜底。
    第二次 Ctrl+C：立即强制退出（os._exit 绕过所有 atexit/finally）。
    """
    interrupt_count = 0
    original_handler = signal.getsignal(signal.SIGINT)

    def _sigint_handler(signum: int, frame) -> None:
        nonlocal interrupt_count
        interrupt_count += 1

        if interrupt_count == 1:
            service.request_shutdown()
            # 直接写 stderr，避免与 tqdm 输出混淆
            sys.stderr.write('\n' + t('interrupt_cleaning') + '\n')
            sys.stderr.flush()
            # 看门狗：5 秒后若任务仍未完成则强制退出
            _start_watchdog(timeout=5)
        else:
            sys.stderr.write('\n' + t('interrupt_force') + '\n')
            sys.stderr.flush()
            _release_tqdm_mp_lock()
            os._exit(1)

    signal.signal(signal.SIGINT, _sigint_handler)
    return original_handler


def _start_watchdog(timeout: float) -> None:
    """启动守护线程，在 timeout 秒后若进程仍在运行则强制终止。"""
    def _watchdog() -> None:
        time.sleep(timeout)
        _release_tqdm_mp_lock()
        os._exit(1)

    thread = threading.Thread(target=_watchdog, daemon=True, name='shutdown-watchdog')
    thread.start()


def _resolve_dataset_token(client: APIClient, identifier: str) -> str:
    """将 dataset 标识符（token 或 name）解析为 dataset token。

    调用 GET /partner/datasets/{identifier} 接口（服务端支持 token 或 name 精确匹配），
    成功时返回真实 token；失败时直接将原值当 token 使用（错误由后续接口返回）。
    """
    try:
        resp = client.get(f'/partner/datasets/{identifier}')
        if resp.get('code') in (0, '0', '0000'):
            data = resp.get('data', {})
            ds = data.get('dataset', data)
            resolved = ds.get('token')
            if resolved and resolved != identifier:
                typer.echo(t('dataset_resolved', identifier, resolved))
            return resolved or identifier
    except Exception:
        pass
    return identifier


def download(
    ctx: typer.Context,
    dataset_identifier: str = typer.Argument(None, help=t('help_download_token')),
    output_dir: str = typer.Option('./data', help=t('help_download_output_dir')),
    concurrency: int = typer.Option(4, help=t('help_download_concurrency')),
    organize_by_sst: bool = typer.Option(True, help=t('help_download_organize_by_sst')),
    skip_existing: bool = typer.Option(True, help=t('help_download_skip_existing')),
    verbose: bool = typer.Option(False, '--verbose', help=t('help_download_verbose')),
    quiet: bool = typer.Option(False, '--quiet', help=t('help_download_quiet')),
):
    """下载数据集"""
    if not dataset_identifier:
        typer.echo(ctx.get_help())
        raise typer.Exit()

    setup_logger(verbose, quiet)

    config = load_config()
    if not config.access_token:
        typer.echo(t('not_logged_in'))
        raise typer.Exit(EXIT_TOKEN_INVALID)

    if not 1 <= concurrency <= 16:
        typer.echo(t('concurrency_range_error'))
        raise typer.Exit(EXIT_INVALID_ARGS)

    # 先创建 service 获取 run_id，再创建携带 run_id 的 client
    service = DownloadService(client=None, concurrency=concurrency)
    client = APIClient(
        config.api_base_url, config.access_token, run_id=service.run_id
    )
    service.client = client

    # 解析 dataset 标识符（支持 token 或 name）
    dataset_token = _resolve_dataset_token(client, dataset_identifier)

    original_handler = _install_sigint_handler(service)
    try:
        output_path = Path(output_dir)
        exit_code = service.download_dataset(
            dataset_token, output_path, organize_by_sst, skip_existing
        )
    finally:
        # 还原原始信号处理器，避免影响 typer 后续的清理逻辑
        signal.signal(signal.SIGINT, original_handler)

    raise typer.Exit(exit_code)
