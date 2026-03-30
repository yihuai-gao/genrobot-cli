import typer
from datetime import datetime, timedelta, timezone

from genrobot.config import load_config, save_config
from genrobot.exit_codes import EXIT_TOKEN_INVALID, EXIT_NETWORK_ERROR
from genrobot.http_client import APIClient
from genrobot.i18n import t

app = typer.Typer(
    context_settings={'help_option_names': ['-h', '--help']},
    invoke_without_command=True,
)


@app.callback()
def _auth_callback(ctx: typer.Context) -> None:
    """Authentication"""
    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()


@app.command(help=t('help_auth_login'))
def login(
    username: str = typer.Option(None, prompt=True, help=t('help_auth_login_username')),
    password: str = typer.Option(None, prompt=True, hide_input=True, help=t('help_auth_login_password')),
    endpoint: str = typer.Option(None, help=t('help_auth_login_endpoint')),
):
    """登录认证"""
    config = load_config()
    if endpoint:
        config.api_base_url = endpoint
    client = APIClient(config.api_base_url)

    try:
        resp = client.post('/partner/auth/login', json={
            'username': username,
            'password': password,
        })

        # 服务端 code 可能是 0 / '0' / '0000' / '-1'
        code = resp.get('code')
        if code not in (0, '0', '0000'):
            typer.echo(t('login_failed'))
            raise typer.Exit(EXIT_TOKEN_INVALID)

        data = resp.get('data', {})
        config.access_token = data.get('token') or data.get('access_token')
        config.refresh_token = data.get('refreshToken') or data.get('refresh_token')
        config.username = username

        # 保存 token 过期时间
        config.token_expires_at = (
            datetime.now(timezone.utc) + timedelta(days=30)
        ).isoformat()

        save_config(config)
        typer.echo(t('login_success'))

    except typer.Exit:
        raise
    except Exception:
        typer.echo(t('network_error'))
        raise typer.Exit(EXIT_NETWORK_ERROR)


@app.command(help=t('help_auth_whoami'))
def whoami():
    """查看当前登录用户"""
    config = load_config()
    if not config.access_token:
        typer.echo(t('not_logged_in'))
        raise typer.Exit(1)

    client = APIClient(config.api_base_url, config.access_token)
    try:
        resp = client.get('/partner/auth/getUserInfo')
        data = resp.get('data', {})
        typer.echo(t('current_user', data.get('username') or data.get('name', 'N/A')))
        typer.echo(t('partner_id', data.get('id') or data.get('partner_id', 'N/A')))

        if config.token_expires_at:
            try:
                exp = datetime.fromisoformat(config.token_expires_at.replace('Z', '+00:00'))
                remaining = (exp - datetime.now(timezone.utc)).days
                typer.echo(t('token_expires', exp.strftime('%Y-%m-%d %H:%M'), remaining))
            except (ValueError, TypeError):
                pass
    except Exception as e:
        typer.echo(f'✗ {e}')
        raise typer.Exit(1)


@app.command(help=t('help_auth_logout'))
def logout():
    """退出登录"""
    config = load_config()
    config.access_token = None
    config.refresh_token = None
    config.token_expires_at = None
    config.username = None
    config.partner_id = None
    save_config(config)
    typer.echo(t('logged_out'))
