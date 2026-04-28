from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
CHECK_SCRIPT = ROOT / "scripts" / "check_phase1_environment.py"


def load_check_module():
    spec = importlib.util.spec_from_file_location(
        "check_phase1_environment",
        CHECK_SCRIPT,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


check_phase1_environment = load_check_module()


def test_check_passes_with_required_prerequisites_and_warns_for_missing_fmp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bin_dir = _fake_executable(tmp_path, "psql")
    expected_paths = (
        check_phase1_environment.ExpectedPath("pyproject.toml", "file"),
        check_phase1_environment.ExpectedPath("scripts", "dir"),
    )
    _create_expected_paths(tmp_path, expected_paths)
    database_url = "postgresql://user:super-secret@localhost:5432/silver"

    monkeypatch.setenv("PATH", str(bin_dir))
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.delenv("FMP_API_KEY", raising=False)

    results = check_phase1_environment.collect_checks(
        root=tmp_path,
        env=os.environ,
        required_imports=("present_module",),
        expected_paths=expected_paths,
        find_spec=lambda name: object(),
    )

    rendered = check_phase1_environment.format_results(results)
    assert check_phase1_environment.exit_code(results) == 0
    assert "WARN: environment FMP_API_KEY: not set" in rendered
    assert "OK: environment DATABASE_URL: set; value hidden" in rendered
    assert database_url not in rendered


def test_missing_hard_prerequisites_return_nonzero(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    empty_bin = tmp_path / "empty-bin"
    empty_bin.mkdir()
    monkeypatch.setenv("PATH", str(empty_bin))
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("FMP_API_KEY", raising=False)

    results = check_phase1_environment.collect_checks(
        root=tmp_path,
        env=os.environ,
        required_imports=("missing_module",),
        expected_paths=(
            check_phase1_environment.ExpectedPath("scripts/run_falsifier.py", "file"),
        ),
        find_spec=lambda name: None,
    )

    rendered = check_phase1_environment.format_results(results)
    assert check_phase1_environment.exit_code(results) == 1
    assert "FAIL: command psql: missing from PATH" in rendered
    assert "FAIL: Python import missing_module: unavailable" in rendered
    assert "FAIL: environment DATABASE_URL: not set" in rendered
    assert (
        "FAIL: repo path scripts/run_falsifier.py: missing expected file" in rendered
    )


def test_set_optional_secret_is_reported_without_printing_value(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bin_dir = _fake_executable(tmp_path, "psql")
    expected_paths = (
        check_phase1_environment.ExpectedPath("pyproject.toml", "file"),
    )
    _create_expected_paths(tmp_path, expected_paths)
    fmp_api_key = "fmp-secret"

    monkeypatch.setenv("PATH", str(bin_dir))
    monkeypatch.setenv("DATABASE_URL", "postgresql://localhost:5432/silver")
    monkeypatch.setenv("FMP_API_KEY", fmp_api_key)

    results = check_phase1_environment.collect_checks(
        root=tmp_path,
        env=os.environ,
        required_imports=(),
        expected_paths=expected_paths,
    )

    rendered = check_phase1_environment.format_results(results)
    assert "OK: environment FMP_API_KEY: set; value hidden" in rendered
    assert fmp_api_key not in rendered


def _fake_executable(tmp_path: Path, name: str) -> Path:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    executable = bin_dir / name
    executable.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    executable.chmod(0o755)
    return bin_dir


def _create_expected_paths(
    root: Path,
    expected_paths: tuple[object, ...],
) -> None:
    for expected in expected_paths:
        path = root / expected.relative_path
        if expected.kind == "file":
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("", encoding="utf-8")
        else:
            path.mkdir(parents=True, exist_ok=True)
