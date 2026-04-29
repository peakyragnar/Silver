from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORK_LEDGER_SCRIPT = ROOT / "scripts" / "work_ledger.py"


def load_work_ledger_module():
    spec = importlib.util.spec_from_file_location(
        "work_ledger",
        WORK_LEDGER_SCRIPT,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


work_ledger = load_work_ledger_module()


def test_init_status_and_import_objectives(tmp_path: Path) -> None:
    root = _write_repo_with_objective(tmp_path)
    ledger_path = tmp_path / "ledger.db"

    work_ledger.initialize_ledger(ledger_path)
    with work_ledger.connect_existing(ledger_path) as connection:
        result = work_ledger.import_objectives(
            connection,
            work_ledger.active_objective_proposals(root),
        )
        status = work_ledger.ledger_status(connection, ledger_path)

    assert result.objectives_imported == 1
    assert result.tickets_created == 3
    assert result.tickets_updated == 0
    assert status["objectives"] == 1
    assert status["tickets"] == 3
    assert status["statuses"] == {"Backlog": 3}


def test_import_is_idempotent_and_preserves_status(tmp_path: Path) -> None:
    root = _write_repo_with_objective(tmp_path)
    ledger_path = tmp_path / "ledger.db"
    work_ledger.initialize_ledger(ledger_path)

    with work_ledger.connect_existing(ledger_path) as connection:
        work_ledger.import_objectives(
            connection,
            work_ledger.active_objective_proposals(root),
        )
        work_ledger.transition_ticket(
            connection,
            ticket_id="local-runtime-ledger-001",
            status="Ready",
            actor="test",
            message="ready",
        )
        result = work_ledger.import_objectives(
            connection,
            work_ledger.active_objective_proposals(root),
        )
        ticket = work_ledger.select_ticket(connection, "local-runtime-ledger-001")

    assert result.tickets_created == 0
    assert result.tickets_updated == 3
    assert ticket.status == "Ready"


def test_admit_promotes_ready_tickets_and_lists_runnable(tmp_path: Path) -> None:
    root = _write_repo_with_objective(tmp_path)
    ledger_path = tmp_path / "ledger.db"
    work_ledger.initialize_ledger(ledger_path)

    with work_ledger.connect_existing(ledger_path) as connection:
        work_ledger.import_objectives(
            connection,
            work_ledger.active_objective_proposals(root),
        )
        result = work_ledger.admit_backlog_tickets(
            connection,
            max_active=5,
            ready_buffer=5,
            dry_run=False,
        )
        runnable = work_ledger.runnable_tickets(connection)

    assert [ticket.id for ticket in result.promoted] == [
        "local-runtime-ledger-001",
        "local-runtime-ledger-003",
    ]
    assert [(ticket.id, reason) for ticket, reason in result.waiting] == [
        (
            "local-runtime-ledger-002",
            "migration lane occupied by local-runtime-ledger-001",
        )
    ]
    assert [ticket.id for ticket in runnable] == [
        "local-runtime-ledger-001",
        "local-runtime-ledger-003",
    ]


def test_transition_records_event_and_json_output(tmp_path: Path) -> None:
    root = _write_repo_with_objective(tmp_path)
    ledger_path = tmp_path / "ledger.db"
    work_ledger.initialize_ledger(ledger_path)

    with work_ledger.connect_existing(ledger_path) as connection:
        work_ledger.import_objectives(
            connection,
            work_ledger.active_objective_proposals(root),
        )
        ticket = work_ledger.transition_ticket(
            connection,
            ticket_id="local-runtime-ledger-003",
            status="Ready",
            actor="test",
            message="unit test transition",
        )
        events = work_ledger.recent_events(
            connection,
            ticket_id="local-runtime-ledger-003",
            limit=1,
        )

    payload = json.loads(work_ledger.render_transition(ticket, "json"))

    assert payload["status"] == "Ready"
    assert events[0]["event_type"] == "status_transition"
    assert events[0]["from_status"] == "Backlog"
    assert events[0]["to_status"] == "Ready"
    assert events[0]["message"] == "unit test transition"


def test_cli_import_and_status(tmp_path: Path, capsys) -> None:
    root = _write_repo_with_objective(tmp_path)
    ledger_path = tmp_path / "ledger.db"

    assert work_ledger.main(["--ledger", str(ledger_path), "init"]) == 0
    assert (
        work_ledger.main(
            [
                "--ledger",
                str(ledger_path),
                "--root",
                str(root),
                "import-objectives",
            ]
        )
        == 0
    )
    assert (
        work_ledger.main(["--ledger", str(ledger_path), "status", "--format", "json"])
        == 0
    )

    output = capsys.readouterr().out
    assert "OK: work ledger initialized" in output
    assert "Tickets created: 3" in output
    assert '"tickets": 3' in output


def _write_repo_with_objective(root: Path) -> Path:
    active_dir = root / "docs" / "objectives" / "active"
    completed_dir = root / "docs" / "objectives" / "completed"
    active_dir.mkdir(parents=True)
    completed_dir.mkdir(parents=True)
    (root / "docs" / "Symphony-Operation.md").write_text(
        "# Operation\n\nPlanning Steward\nObjective Impact\n",
        encoding="utf-8",
    )
    (root / "WORKFLOW.md").write_text(
        "## Proof Packet\n\n- Objective Impact\n",
        encoding="utf-8",
    )
    (root / "docs" / "exec-plans" / "active").mkdir(parents=True)
    (root / "db" / "migrations").mkdir(parents=True)
    (root / "src").mkdir()
    (root / "scripts").mkdir()
    (root / "scripts" / "planning_steward.py").write_text("# test\n")
    (active_dir / "local-runtime-ledger.md").write_text(
        "\n".join(
            [
                "# local-runtime-ledger",
                "",
                "Objective:",
                "Create the local work ledger for fast Silver orchestration.",
                "",
                "User Value:",
                "Michael can run agents from local state instead of waiting on Linear.",
                "",
                "Why Now:",
                "Linear should become a mirror, not the runtime database.",
                "",
                "Done When:",
                "- Active Objectives can be imported into the ledger.",
                "- Runnable tickets can be listed locally.",
                "",
                "Out Of Scope:",
                "- No Symphony dispatcher replacement.",
                "- No Linear writes.",
                "",
                "Guardrails:",
                "- Keep the current Linear bridge working.",
                "- Do not commit local ledger databases.",
                "",
                "Expected Tickets:",
                "- Add ledger schema",
                "  Purpose: Store Objective and ticket state locally.",
                "  Objective Impact: The Objective becomes executable without relying on Linear polling.",
                "  Technical Summary: Create the SQLite tables and import path.",
                "  Owns:",
                "  - db/migrations/",
                "  Conflict Zones:",
                "  - db/migrations/",
                "  Validation:",
                "  - python -m pytest tests/test_work_ledger.py",
                "- Add second schema task",
                "  Purpose: Prove migration-lane serialization.",
                "  Objective Impact: The ledger prevents parallel schema ownership before agents run.",
                "  Technical Summary: Try to admit a second migration owner.",
                "  Owns:",
                "  - db/migrations/",
                "  Conflict Zones:",
                "  - db/migrations/",
                "  Validation:",
                "  - python -m pytest tests/test_work_ledger.py",
                "- Add ledger docs",
                "  Purpose: Explain the local ledger operator path.",
                "  Objective Impact: Michael can inspect fast local state before Linear mirroring.",
                "  Technical Summary: Document CLI commands and the migration path.",
                "  Owns:",
                "  - docs/Symphony-Operation.md",
                "  Conflict Zones:",
                "  - docs/",
                "  Validation:",
                "  - git diff --check",
                "",
                "Validation:",
                "- git diff --check",
                "- python -m pytest tests/test_work_ledger.py",
                "",
                "Conflict Zones:",
                "- scripts/work_ledger.py",
                "- tests/test_work_ledger.py",
                "- docs/Symphony-Operation.md",
            ]
        ),
        encoding="utf-8",
    )
    return root
