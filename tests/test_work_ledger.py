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
        first_ticket = work_ledger.select_ticket(
            connection,
            "local-runtime-ledger-001",
        )

    assert result.objectives_imported == 1
    assert result.tickets_created == 3
    assert result.tickets_updated == 0
    assert status["objectives"] == 1
    assert status["tickets"] == 3
    assert status["statuses"] == {"Backlog": 3}
    assert status["schema_version"] == 2
    assert first_ticket.ticket_role == "contract"
    assert first_ticket.dependency_group == "default"
    assert first_ticket.contracts_touched
    assert first_ticket.risk_class == "migration"


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


def test_contract_first_dag_admission_gates_parallel_work(
    tmp_path: Path,
) -> None:
    root = _write_repo_with_dag_objective(tmp_path)
    ledger_path = tmp_path / "ledger.db"
    work_ledger.initialize_ledger(ledger_path)

    with work_ledger.connect_existing(ledger_path) as connection:
        work_ledger.import_objectives(
            connection,
            work_ledger.active_objective_proposals(root),
        )

        first_pass = work_ledger.admit_backlog_tickets(
            connection,
            max_active=10,
            ready_buffer=10,
            dry_run=False,
        )
        work_ledger.transition_ticket(
            connection,
            ticket_id="portable-orchestration-core-001",
            status="Done",
            actor="test",
            message="contract accepted",
        )

        second_pass = work_ledger.admit_backlog_tickets(
            connection,
            max_active=10,
            ready_buffer=10,
            dry_run=False,
        )
        work_ledger.transition_ticket(
            connection,
            ticket_id="portable-orchestration-core-002",
            status="Done",
            actor="test",
            message="implementation accepted",
        )
        work_ledger.transition_ticket(
            connection,
            ticket_id="portable-orchestration-core-003",
            status="Done",
            actor="test",
            message="implementation accepted",
        )

        third_pass = work_ledger.admit_backlog_tickets(
            connection,
            max_active=10,
            ready_buffer=10,
            dry_run=False,
        )
        work_ledger.transition_ticket(
            connection,
            ticket_id="portable-orchestration-core-004",
            status="Done",
            actor="test",
            message="integration accepted",
        )

        fourth_pass = work_ledger.admit_backlog_tickets(
            connection,
            max_active=10,
            ready_buffer=10,
            dry_run=False,
        )

    assert [ticket.id for ticket in first_pass.promoted] == [
        "portable-orchestration-core-001",
    ]
    assert [(ticket.id, reason) for ticket, reason in first_pass.waiting] == [
        (
            "portable-orchestration-core-002",
            "blocked by unfinished contract ticket: portable-orchestration-core-001",
        ),
        (
            "portable-orchestration-core-003",
            "blocked by unfinished contract ticket: portable-orchestration-core-001",
        ),
        (
            "portable-orchestration-core-004",
            "blocked by unfinished contract ticket: portable-orchestration-core-001",
        ),
        (
            "portable-orchestration-core-005",
            "blocked by unfinished contract ticket: portable-orchestration-core-001",
        ),
    ]
    assert [ticket.id for ticket in second_pass.promoted] == [
        "portable-orchestration-core-002",
        "portable-orchestration-core-003",
    ]
    assert [(ticket.id, reason) for ticket, reason in second_pass.waiting] == [
        (
            "portable-orchestration-core-004",
            "blocked by unfinished implementation ticket: portable-orchestration-core-002",
        ),
        (
            "portable-orchestration-core-005",
            "blocked by unfinished implementation ticket: portable-orchestration-core-002",
        ),
    ]
    assert [ticket.id for ticket in third_pass.promoted] == [
        "portable-orchestration-core-004",
    ]
    assert [(ticket.id, reason) for ticket, reason in third_pass.waiting] == [
        (
            "portable-orchestration-core-005",
            "blocked by unfinished integration ticket: portable-orchestration-core-004",
        )
    ]
    assert [ticket.id for ticket in fourth_pass.promoted] == [
        "portable-orchestration-core-005",
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


def _write_repo_with_dag_objective(root: Path) -> Path:
    _write_minimal_ledger_repo(root)
    active_dir = root / "docs" / "objectives" / "active"
    (active_dir / "portable-orchestration-core.md").write_text(
        "\n".join(
            [
                "# portable-orchestration-core",
                "",
                "Objective:",
                "Build the portable objective-to-ticket orchestration core.",
                "",
                "User Value:",
                "Michael approves Objectives while the system compiles safe parallel work.",
                "",
                "Why Now:",
                "Ticket-by-ticket execution needs objective planning and integration context.",
                "",
                "Done When:",
                "- Objectives compile into contract-gated ticket DAGs.",
                "- Integration and validation wait for the right upstream work.",
                "",
                "Out Of Scope:",
                "- No new runner implementation.",
                "",
                "Guardrails:",
                "- Keep Linear as a mirror.",
                "",
                "Expected Tickets:",
                "- Define objective DAG contract",
                "  Ticket Role: contract",
                "  Dependency Group: orchestration-core",
                "  Contracts Touched:",
                "  - objective-dag",
                "  Risk Class: low",
                "  Purpose: Define the portable ticket DAG contract.",
                "  Objective Impact: Downstream implementation tickets build against one accepted graph shape.",
                "  Technical Summary: Specify roles, dependency groups, and contract metadata.",
                "  Owns:",
                "  - docs/AGENTIC_BUILD_SYSTEM_CORE.md",
                "  Validation:",
                "  - git diff --check",
                "- Parse objective DAG metadata",
                "  Ticket Role: implementation",
                "  Dependency Group: orchestration-core",
                "  Contracts Touched:",
                "  - objective-dag",
                "  Purpose: Parse role and contract metadata from Objective files.",
                "  Objective Impact: Objective files become executable graph inputs.",
                "  Technical Summary: Extend the planning steward parser.",
                "  Owns:",
                "  - scripts/planning_steward.py",
                "  Validation:",
                "  - python -m pytest tests/test_planning_steward.py",
                "- Store objective DAG metadata",
                "  Ticket Role: implementation",
                "  Dependency Group: orchestration-core",
                "  Contracts Touched:",
                "  - objective-dag",
                "  Purpose: Persist role and contract metadata in the local ledger.",
                "  Objective Impact: Admission decisions can use the Objective graph.",
                "  Technical Summary: Extend the work ledger schema and payloads.",
                "  Owns:",
                "  - scripts/work_ledger.py",
                "  Validation:",
                "  - python -m pytest tests/test_work_ledger.py",
                "- Integrate objective DAG metadata",
                "  Ticket Role: integration",
                "  Dependency Group: orchestration-core",
                "  Contracts Touched:",
                "  - objective-dag",
                "  Purpose: Reconcile the compiler, ledger, and mirror outputs.",
                "  Objective Impact: The system treats Objective output as one coherent graph.",
                "  Technical Summary: Verify graph metadata flows through every adapter boundary.",
                "  Owns:",
                "  - scripts/linear_mirror.py",
                "  Validation:",
                "  - python -m pytest tests/test_linear_mirror.py",
                "- Validate objective orchestration path",
                "  Ticket Role: validation",
                "  Dependency Group: orchestration-core",
                "  Contracts Touched:",
                "  - objective-dag",
                "  Purpose: Prove contract-first fanout and integration gating.",
                "  Objective Impact: Michael can trust the next Silver Objectives to run in order.",
                "  Technical Summary: Run the focused orchestration tests.",
                "  Owns:",
                "  - tests/",
                "  Validation:",
                "  - python -m pytest tests/test_work_ledger.py",
                "",
                "Validation:",
                "- git diff --check",
                "- python -m pytest tests/test_work_ledger.py",
                "",
                "Conflict Zones:",
                "- scripts/planning_steward.py",
                "- scripts/work_ledger.py",
                "- scripts/linear_mirror.py",
            ]
        ),
        encoding="utf-8",
    )
    return root


def _write_minimal_ledger_repo(root: Path) -> None:
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
