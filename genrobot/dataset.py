import unicodedata

import typer

from genrobot.config import load_config
from genrobot.http_client import APIClient
from genrobot.i18n import t

app = typer.Typer(
    context_settings={'help_option_names': ['-h', '--help']},
    invoke_without_command=True,
)


@app.callback()
def _dataset_callback(ctx: typer.Context) -> None:
    """Dataset management"""
    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()


def _format_size(size_bytes: int) -> str:
    """将字节数格式化为人类可读的大小"""
    if size_bytes >= 1024 ** 4:
        return f'{size_bytes / 1024 ** 4:.2f} TB'
    if size_bytes >= 1024 ** 3:
        return f'{size_bytes / 1024 ** 3:.2f} GB'
    if size_bytes >= 1024 ** 2:
        return f'{size_bytes / 1024 ** 2:.1f} MB'
    return f'{size_bytes / 1024:.0f} KB'


def _format_duration(seconds: float) -> str:
    """将秒数格式化为人类可读的时长"""
    if seconds >= 3600:
        return f'{seconds / 3600:.1f}h'
    if seconds >= 60:
        return f'{seconds / 60:.1f}min'
    return f'{seconds:.1f}s'


def _display_width(s: str) -> int:
    """计算字符串在终端的显示宽度（CJK 宽字符占 2 列）"""
    w = 0
    for ch in s:
        w += 2 if unicodedata.east_asian_width(ch) in ('W', 'F') else 1
    return w


def _truncate(s: str, max_width: int) -> str:
    """按显示宽度截断字符串，超出时以 '…' 结尾"""
    w = 0
    for i, ch in enumerate(s):
        cw = 2 if unicodedata.east_asian_width(ch) in ('W', 'F') else 1
        if w + cw > max_width - 1:
            return s[:i] + '…'
        w += cw
    return s


def _pad_right(s: str, width: int) -> str:
    """右填充空格至指定显示宽度"""
    return s + ' ' * max(0, width - _display_width(s))


def _pad_left(s: str, width: int) -> str:
    """左填充空格至指定显示宽度"""
    return ' ' * max(0, width - _display_width(s)) + s


@app.command('list', help=t('help_dataset_list'))
def list_datasets():
    """列出已订阅的数据集"""
    config = load_config()
    if not config.access_token:
        typer.echo(t('not_logged_in'))
        raise typer.Exit(1)

    client = APIClient(config.api_base_url, config.access_token)
    resp = client.get('/partner/datasets')

    data = resp.get('data', {})
    datasets = data.get('datasets', [])
    if not datasets:
        typer.echo(t('no_datasets'))
        return

    # 自适应列宽：name 和 token 根据实际数据计算
    col_name_w = max(
        30,
        _display_width(t('col_name')) + 2,
        *(min(40, _display_width(ds.get('name', ''))) + 2 for ds in datasets),
    )
    col_name_w = min(col_name_w, 40)

    col_token_w = max(
        20,
        len(t('col_token')) + 2,
        *(min(36, len(ds.get('token', ''))) + 2 for ds in datasets),
    )
    col_token_w = min(col_token_w, 36)

    # 固定宽度列
    col_dur_w = 10
    col_size_w = 10
    col_files_w = 10
    col_sc_w = 10
    col_sk_w = 8
    sep = '  '
    total_w = col_name_w + col_token_w + col_dur_w + col_size_w + col_files_w + col_sc_w + col_sk_w + len(sep) * 6

    typer.echo(t('available_datasets'))
    typer.echo('─' * total_w)

    header = (
        _pad_right(t('col_name'), col_name_w) + sep
        + _pad_right(t('col_token'), col_token_w) + sep
        + _pad_left(t('col_duration'), col_dur_w) + sep
        + _pad_left(t('col_size'), col_size_w) + sep
        + _pad_left(t('col_files'), col_files_w) + sep
        + _pad_left(t('col_scenarios'), col_sc_w) + sep
        + _pad_left(t('col_skills'), col_sk_w)
    )
    typer.echo(header)
    typer.echo('─' * total_w)

    for ds in datasets:
        name = ds.get('name', 'N/A')
        if _display_width(name) > col_name_w:
            name = _truncate(name, col_name_w)

        token = ds.get('token', 'N/A')
        if len(token) > col_token_w:
            token = token[:col_token_w - 1] + '…'

        duration_str = _format_duration(float(ds.get('total_duration', 0) or 0))
        size_str = _format_size(int(ds.get('total_size', 0) or 0))
        files_str = f'{int(ds.get("data_counts", 0) or 0):,}'
        sc_str = str(ds.get('total_scenario_count', 0))
        sk_str = str(ds.get('total_skill_count', 0))

        row = (
            _pad_right(name, col_name_w) + sep
            + _pad_right(token, col_token_w) + sep
            + _pad_left(duration_str, col_dur_w) + sep
            + _pad_left(size_str, col_size_w) + sep
            + _pad_left(files_str, col_files_w) + sep
            + _pad_left(sc_str, col_sc_w) + sep
            + _pad_left(sk_str, col_sk_w)
        )
        typer.echo(row)


@app.command('stats', help=t('help_dataset_stats'))
def dataset_stats(
    ctx: typer.Context,
    dataset_identifier: str = typer.Argument(None, help=t('help_dataset_identifier')),
):
    """查看数据集统计信息"""
    if not dataset_identifier:
        typer.echo(ctx.get_help())
        raise typer.Exit()

    config = load_config()
    if not config.access_token:
        typer.echo(t('not_logged_in'))
        raise typer.Exit(1)

    client = APIClient(config.api_base_url, config.access_token)
    resp = client.get(f'/partner/datasets/{dataset_identifier}')

    code = resp.get('code')
    if code not in (0, '0', '0000'):
        error_code = resp.get('data', {}).get('error_code', '')
        if error_code in ('NOT_SUBSCRIBED', 'SUBSCRIPTION_INACTIVE', 'SUBSCRIPTION_EXPIRED'):
            typer.echo(t('not_subscribed'))
        else:
            typer.echo(f'✗ {resp.get("message", "Unknown error")}')
        raise typer.Exit(1)

    data = resp.get('data', {})
    ds = data.get('dataset', data)
    subscription = data.get('subscription') or ds.get('subscription', {})

    # 两列 KV 格式：label 列固定显示宽度（CJK 感知）
    label_width = 18

    def _kv(label: str, value: str) -> str:
        return f'  {_pad_right(label, label_width)}{value}'

    sep = '─' * (label_width + 32)

    # --- 数据集详情 ---
    typer.echo(t('stats_section_dataset'))
    typer.echo(sep)
    typer.echo(_kv('Token', ds.get('token', dataset_identifier)))
    typer.echo(_kv(t('col_name'), ds.get('name', 'N/A')))
    desc = ds.get('description', '')
    if desc:
        typer.echo(_kv(t('stats_label_description'), desc))

    # --- 统计信息 ---
    typer.echo('')
    typer.echo(t('stats_section_statistics'))
    typer.echo(sep)
    data_counts = int(ds.get('data_counts', 0) or 0)
    total_size = int(ds.get('total_size', 0) or 0)
    total_duration = float(ds.get('total_duration', 0) or 0)
    scenario_count = ds.get('total_scenario_count')
    skill_count = ds.get('total_skill_count')

    typer.echo(_kv(t('col_files'), f'{data_counts:,}'))
    typer.echo(_kv(t('stats_label_total_size'), _format_size(total_size)))
    typer.echo(_kv(t('stats_total_duration'), _format_duration(total_duration)))
    if scenario_count is not None:
        typer.echo(_kv(t('stats_scenario_count'), str(scenario_count)))
    if skill_count is not None:
        typer.echo(_kv(t('stats_skill_count'), str(skill_count)))

    # --- 订阅信息 ---
    if subscription:
        typer.echo('')
        typer.echo(t('stats_section_subscription'))
        typer.echo(sep)
        purchase_time = subscription.get('purchase_time', '')
        if purchase_time:
            typer.echo(_kv(t('stats_purchase_time'), purchase_time[:10]))
        expire_time = subscription.get('expire_time', '')
        if expire_time:
            typer.echo(_kv(t('stats_label_expires'), expire_time[:10]))
        days_remaining = subscription.get('days_remaining')
        if days_remaining is not None:
            typer.echo(_kv(t('stats_days_remaining'), str(days_remaining)))
