"""Local checks for Stage IV files (no cluster).

Covered by ``pylint scripts`` on the grader. Manual run:
``python scripts/stage4_validate.py``
"""
from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REQUIRED = (
    ROOT / "scripts" / "stage4.sh",
    ROOT / "sql" / "stage4_ml_dashboard.hql",
    ROOT / "sql" / "data" / "stage4_ml_features.csv",
    ROOT / "sql" / "data" / "stage4_ml_hyper.csv",
)


def main() -> int:
    """Verify Stage IV assets exist and ``stage4.sh`` passes ``bash -n``."""
    missing = [str(p.relative_to(ROOT)) for p in REQUIRED if not p.is_file()]
    if missing:
        print("stage4_validate: missing files:", file=sys.stderr)
        for m in missing:
            print(" ", m, file=sys.stderr)
        return 1
    stage4_sh = ROOT / "scripts" / "stage4.sh"
    if stage4_sh.stat().st_size < 50:
        print("stage4_validate: stage4.sh looks empty", file=sys.stderr)
        return 1
    bash = _bash()
    if bash:
        subprocess.run([bash, "-n", str(stage4_sh)], check=True)
    return 0


def _bash() -> str | None:
    """Return path to bash if available and responsive, else None."""
    exe = shutil.which("bash")
    if not exe:
        return None
    try:
        subprocess.run(
            [exe, "--version"],
            check=True,
            capture_output=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired, subprocess.CalledProcessError):
        return None
    return exe


if __name__ == "__main__":
    sys.exit(main())
