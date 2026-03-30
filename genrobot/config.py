import json
import os
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional

CONFIG_DIR = Path.home() / '.genrobot'
CONFIG_FILE = CONFIG_DIR / 'config.json'

# 可通过环境变量 GENROBOT_API_BASE_URL 覆盖
_DEFAULT_API_BASE_URL = 'https://api.genrobot.click'


@dataclass
class Config:
    api_base_url: str = ''
    access_token: Optional[str] = None
    refresh_token: Optional[str] = None
    token_expires_at: Optional[str] = None
    partner_id: Optional[int] = None
    username: Optional[str] = None
    default_concurrency: int = 4

    def __post_init__(self):
        if not self.api_base_url:
            self.api_base_url = os.environ.get(
                'GENROBOT_API_BASE_URL', _DEFAULT_API_BASE_URL
            )


def load_config() -> Config:
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, encoding='utf-8') as f:
            data = json.load(f)
        # 只取 Config 定义的字段，忽略未知字段
        valid_fields = {fld.name for fld in Config.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in valid_fields}
        return Config(**filtered)
    return Config()


def save_config(config: Config) -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(asdict(config), f, indent=2, ensure_ascii=False)
    # 仅当前用户可读写
    CONFIG_FILE.chmod(0o600)
