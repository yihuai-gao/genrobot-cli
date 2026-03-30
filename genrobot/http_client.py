import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any

import requests

from genrobot.config import load_config, save_config

logger = logging.getLogger('genrobot')


class RateLimitedError(Exception):
    """限流异常，携带 retry_after_seconds"""

    def __init__(self, retry_after: int, reason_code: str = ''):
        self.retry_after = retry_after
        self.reason_code = reason_code
        super().__init__(f'Rate limited, retry after {retry_after}s')


class APIError(Exception):
    """API 业务错误"""

    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(message)


class APIClient:
    """HTTP 客户端，封装认证、Token 自动刷新和限流处理"""

    def __init__(self, base_url: str, access_token: Optional[str] = None,
                 run_id: Optional[str] = None):
        self.base_url = base_url.rstrip('/')
        self.access_token = access_token
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'genrobot-cli/0.1.0',
        })
        if run_id:
            self.session.headers['x-client-run-id'] = run_id
        if access_token:
            self._set_auth(access_token)

    def _set_auth(self, token: str) -> None:
        self.access_token = token
        self.session.headers['Authorization'] = f'Bearer {token}'

    def _should_refresh_token(self) -> bool:
        """Token 是否即将过期（过期前 7 天主动刷新）"""
        config = load_config()
        if not config.token_expires_at:
            return False
        try:
            expires_at = datetime.fromisoformat(
                config.token_expires_at.replace('Z', '+00:00')
            )
            return datetime.now(timezone.utc) >= expires_at - timedelta(days=7)
        except (ValueError, TypeError):
            return False

    def _try_refresh_token(self) -> bool:
        """使用 refresh_token 刷新 access_token"""
        config = load_config()
        if not config.refresh_token:
            return False

        try:
            resp = self.session.post(
                f'{self.base_url}/partner/auth/refreshToken',
                json={'refreshToken': config.refresh_token},
            )
            resp.raise_for_status()
            data = resp.json()

            # 服务端 code 可能是 0 / '0' / '0000'
            code = data.get('code')
            if code in (0, '0', '0000'):
                tokens = data.get('data', {})
                new_token = tokens.get('token') or tokens.get('access_token')
                if not new_token:
                    return False

                config.access_token = new_token
                new_refresh = tokens.get('refreshToken') or tokens.get('refresh_token')
                if new_refresh:
                    config.refresh_token = new_refresh

                # 保守估算过期时间
                config.token_expires_at = (
                    datetime.now(timezone.utc) + timedelta(days=30)
                ).isoformat()
                save_config(config)
                self._set_auth(new_token)
                logger.info('Token refreshed successfully')
                return True
        except Exception as e:
            logger.debug(f'Token refresh failed: {e}')

        return False

    def request(self, method: str, path: str, **kwargs) -> Dict[str, Any]:
        """发送请求，自动处理 Token 刷新和限流"""
        url = f'{self.base_url}{path}'

        # 主动刷新即将过期的 Token
        if self._should_refresh_token():
            self._try_refresh_token()

        try:
            response = self.session.request(method, url, timeout=30, **kwargs)
        except requests.exceptions.ConnectionError:
            raise
        except requests.exceptions.Timeout:
            raise

        # 429 限流
        if response.status_code == 429:
            body = response.json() if response.content else {}
            retry_after = body.get('retry_after_seconds', 60)
            reason_code = body.get('reason_code', '')
            raise RateLimitedError(retry_after, reason_code)

        response.raise_for_status()
        result = response.json()

        # 被动刷新：服务端返回 code=9999（Token 过期）
        code = result.get('code')
        if code == '9999':
            if self._try_refresh_token():
                response = self.session.request(method, url, timeout=30, **kwargs)
                response.raise_for_status()
                result = response.json()
            else:
                raise APIError('9999', 'Token expired, please login again')

        return result

    def get(self, path: str, **kwargs) -> Dict[str, Any]:
        return self.request('GET', path, **kwargs)

    def post(self, path: str, **kwargs) -> Dict[str, Any]:
        return self.request('POST', path, **kwargs)
