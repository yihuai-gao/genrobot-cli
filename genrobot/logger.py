import logging
import sys
from datetime import datetime
from pathlib import Path

LOG_DIR = Path.home() / '.genrobot' / 'logs'


def setup_logger(verbose: bool = False, quiet: bool = False) -> logging.Logger:
    """配置日志系统：文件（始终 DEBUG）+ 控制台（受 verbose/quiet 控制）"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    log_file = LOG_DIR / f'download-{datetime.now().strftime("%Y%m%d")}.log'

    logger = logging.getLogger('genrobot')
    # 防止重复添加 handler
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    # 文件 handler（始终 DEBUG，英文日志）
    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))
    logger.addHandler(fh)

    # 控制台 handler
    if not quiet:
        ch = logging.StreamHandler(sys.stderr)
        ch.setLevel(logging.DEBUG if verbose else logging.WARNING)
        ch.setFormatter(logging.Formatter('%(message)s'))
        logger.addHandler(ch)

    return logger
