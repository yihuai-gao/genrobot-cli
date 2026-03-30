import random
import time
from functools import wraps

import requests

# 不可重试的 HTTP 状态码
NON_RETRYABLE_STATUS = {401, 403}


def is_retryable_error(exc: Exception) -> bool:
    """判断异常是否可重试"""
    from genrobot.http_client import RateLimitedError
    from genrobot.download_service import URLExpiredError

    # URL 过期不在单文件级重试，由批次层统一重新签发
    if isinstance(exc, URLExpiredError):
        return False
    if isinstance(exc, RateLimitedError):
        return True
    if isinstance(exc, requests.exceptions.HTTPError):
        return exc.response.status_code not in NON_RETRYABLE_STATUS
    if isinstance(exc, (
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        ValueError,  # size 校验失败
    )):
        return True
    return False


def retry_on_error(max_retries: int = 3, initial_delay: float = 1.0,
                   backoff_factor: float = 2.0, max_delay: float = 32.0):
    """重试装饰器：指数退避 + ±25% 抖动 + 429 retry_after"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            from genrobot.http_client import RateLimitedError

            delay = initial_delay
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1 or not is_retryable_error(e):
                        raise

                    # 429 使用服务端指定的等待时间
                    if isinstance(e, RateLimitedError):
                        sleep_time = e.retry_after
                    else:
                        jitter = delay * 0.25 * (random.random() * 2 - 1)
                        sleep_time = min(delay + jitter, max_delay)
                        delay = min(delay * backoff_factor, max_delay)

                    time.sleep(sleep_time)
            return None
        return wrapper
    return decorator
