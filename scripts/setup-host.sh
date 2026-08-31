#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
ETM_SOURCE=${ETM_SOURCE:-"$ROOT/../../upstream/efb-telegram-master-kettly"}
VENV=${VENV:-"$ROOT/.venv"}
PYTHON_BIN=${PYTHON_BIN:-python3.10}

if [ ! -f "$ETM_SOURCE/setup.py" ]; then
    echo "Kettly ETM source not found: $ETM_SOURCE" >&2
    exit 2
fi

"$PYTHON_BIN" -m venv "$VENV"
. "$VENV/bin/activate"
python -m pip install --upgrade pip
PYTHONUTF8=1 python -m pip install -e "$ETM_SOURCE"
python -m pip install -e "$ROOT"

python - <<'PY'
from pathlib import Path
import efb_telegram_master
import efb_wechat_comwechat_slave

print("Kettly ETM:", Path(efb_telegram_master.__file__).resolve())
print("Linux slave:", Path(efb_wechat_comwechat_slave.__file__).resolve())
PY
