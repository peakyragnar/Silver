from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUN_FALSIFIER_SCRIPT = ROOT / "scripts" / "run_falsifier.py"


def test_check_mode_validates_without_database_url() -> None:
    result = subprocess.run(
        [sys.executable, str(RUN_FALSIFIER_SCRIPT), "--check"],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    assert "OK: falsifier CLI check passed" in result.stdout
    assert "reports/falsifier/week_1_momentum.md" in result.stdout


def test_apply_mode_requires_database_url() -> None:
    result = subprocess.run(
        [sys.executable, str(RUN_FALSIFIER_SCRIPT)],
        text=True,
        capture_output=True,
        check=False,
        env={},
    )

    assert result.returncode == 1
    assert "DATABASE_URL is required unless --check is used" in result.stderr
