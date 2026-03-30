from genrobot.cli import app

try:
    app()
except KeyboardInterrupt:
    raise SystemExit(130)
