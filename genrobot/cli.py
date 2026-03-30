import typer
from typing import Optional

from genrobot import __version__
from genrobot import auth, dataset
from genrobot.download import download as download_cmd
from genrobot.i18n import t

app = typer.Typer(
    help=t('app_help'),
    context_settings={'help_option_names': ['-h', '--help']},
    invoke_without_command=True,
)


def _version_callback(value: bool):
    if value:
        typer.echo(f'genrobot-cli {__version__}')
        raise typer.Exit()


@app.callback()
def main(
    ctx: typer.Context,
    version: Optional[bool] = typer.Option(
        None, '--version', callback=_version_callback,
        is_eager=True, help=t('help_version'),
    ),
):
    """GenRobot CLI"""
    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()


# auth 和 dataset 是命令组（有子命令）
app.add_typer(auth.app, name='auth', help=t('help_auth_group'))
app.add_typer(dataset.app, name='dataset', help=t('help_dataset_group'))

# download 是顶级命令
app.command(name='download', help=t('help_download'))(download_cmd)

if __name__ == '__main__':
    app()
