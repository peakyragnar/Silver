from __future__ import annotations

from dataclasses import replace
import importlib.util
import os
import sqlite3
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OBJECTIVE_RUN_SCRIPT = ROOT / "scripts" / "objective_run.py"


def load_objective_run_module():
    spec = importlib.util.spec_from_file_location(
        "objective_run",
        OBJECTIVE_RUN_SCRIPT,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


objective_run = load_objective_run_module()


def test_command_plan_keeps_symphony_as_runner_bridge(tmp_path: Path) -> None:
    config = _config(tmp_path, apply=False)

    commands = objective_run.command_plan(config)

    assert [command.kind for command in commands] == [
        "import",
        "admit",
        "safety",
        "mirror",
        "vcs",
        "repair",
        "repair",
        "merge",
        "vcs",
        "mirror",
        "status",
    ]
    assert commands[3].argv[1:] == (
        str(ROOT / "scripts" / "linear_mirror.py"),
        "--ledger",
        str(config.ledger),
        "--project",
        "silver-test",
        "--limit",
        "100",
    )
    assert "--dry-run" in commands[0].argv
    assert "--dry-run" in commands[1].argv
    assert "--dry-run" in commands[7].argv
    assert "--apply" not in commands[3].argv


def test_apply_command_plan_writes_through_existing_stewards(
    tmp_path: Path,
) -> None:
    config = _config(
        tmp_path,
        apply=True,
        repair_mode="apply",
        push_repairs=True,
        run_repair_validation=True,
    )

    commands = objective_run.command_plan(config)
    repair_runner = commands[6]

    assert "--apply" in commands[3].argv
    assert "--dry-run" not in commands[7].argv
    assert "--apply" in repair_runner.argv
    assert "--push" in repair_runner.argv
    assert "--run-validation" in repair_runner.argv


def test_runner_observation_advances_only_safe_runner_states() -> None:
    transition = objective_run.decide_runner_state_action(
        ticket_id="obj-001",
        linear_identifier="ARR-1",
        ledger_status="In Progress",
        linear_state="Merging",
    )
    terminal_skip = objective_run.decide_runner_state_action(
        ticket_id="obj-002",
        linear_identifier="ARR-2",
        ledger_status="Done",
        linear_state="In Progress",
    )
    backlog_skip = objective_run.decide_runner_state_action(
        ticket_id="obj-003",
        linear_identifier="ARR-3",
        ledger_status="Backlog",
        linear_state="In Progress",
    )
    unsafe_done = objective_run.decide_runner_state_action(
        ticket_id="obj-004",
        linear_identifier="ARR-4",
        ledger_status="In Progress",
        linear_state="Done",
    )
    safety_pullback = objective_run.decide_runner_state_action(
        ticket_id="obj-005",
        linear_identifier="ARR-5",
        ledger_status="Safety Review",
        linear_state="In Progress",
    )
    backlog_noop = objective_run.decide_runner_state_action(
        ticket_id="obj-006",
        linear_identifier="ARR-6",
        ledger_status="Backlog",
        linear_state="Backlog",
    )

    assert transition.action == "transition"
    assert transition.target_status == "Merging"
    assert terminal_skip.action == "skip"
    assert "terminal ledger" in terminal_skip.reason
    assert backlog_skip.action == "skip"
    assert "admission" in backlog_skip.reason
    assert unsafe_done.action == "skip"
    assert "VCS evidence" in unsafe_done.reason
    assert safety_pullback.action == "skip"
    assert "safety ledger" in safety_pullback.reason
    assert backlog_noop.action == "noop"


def test_apply_runner_state_actions_records_ledger_transitions(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "ledger.db"
    _write_minimal_ledger(ledger, status="In Progress")
    action = objective_run.RunnerStateAction(
        ticket_id="obj-001",
        linear_identifier="ARR-1",
        from_status="In Progress",
        linear_state="Merging",
        action="transition",
        target_status="Merging",
        reason="runner advanced mirrored ticket state",
    )

    with objective_run.work_ledger.connect_existing(ledger) as connection:
        objective_run.apply_runner_state_actions(connection, (action,))
        row = connection.execute(
            "SELECT status FROM tickets WHERE id = 'obj-001'"
        ).fetchone()
        event = connection.execute(
            """
            SELECT actor, to_status, message
            FROM ticket_events
            WHERE ticket_id = 'obj-001'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()

    assert row["status"] == "Merging"
    assert event["actor"] == "objective_run_controller"
    assert event["to_status"] == "Merging"
    assert "ARR-1" in event["message"]


def test_run_cycle_stops_when_a_command_fails(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger.db"
    _write_minimal_ledger(ledger, status="In Progress")
    config = _config(tmp_path, ledger=ledger, apply=True)
    runner = _FakeRunner(fail_kind="vcs")
    observer = _FakeObserver(())

    result = objective_run.run_cycle(
        config,
        cycle=1,
        runner=runner,
        observer=observer,
    )

    assert result.stopped is True
    assert result.stop_reason == "vcs command failed"
    assert [item.kind for item in result.command_results] == [
        "import",
        "admit",
        "safety",
        "mirror",
        "vcs",
    ]


def test_run_cycle_reconciles_safety_blocker_before_dispatch(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "ledger.db"
    _write_minimal_ledger(ledger, status="Safety Review")
    config = _config(tmp_path, ledger=ledger, apply=True)
    runner = _ClearingSafetyRunner(ledger)
    observer = _FakeObserver(())

    result = objective_run.run_cycle(
        config,
        cycle=1,
        runner=runner,
        observer=observer,
    )

    with objective_run.work_ledger.connect_existing(ledger) as connection:
        ticket = objective_run.work_ledger.select_ticket(connection, "obj-001")

    assert result.stopped is False
    assert ticket.status == "Merging"
    assert [item.kind for item in result.command_results[:5]] == [
        "vcs",
        "mirror",
        "import",
        "admit",
        "safety",
    ]


def test_run_cycle_stops_after_reconciliation_when_safety_remains(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "ledger.db"
    _write_minimal_ledger(ledger, status="Safety Review")
    config = _config(tmp_path, ledger=ledger, apply=True)
    runner = _FakeRunner()
    observer = _FakeObserver(())

    result = objective_run.run_cycle(
        config,
        cycle=1,
        runner=runner,
        observer=observer,
    )

    assert result.stopped is True
    assert result.stop_reason == "safety blockers present after VCS reconciliation"
    assert [item.kind for item in result.command_results] == ["vcs", "mirror"]


def test_preflight_reports_missing_required_environment(tmp_path: Path) -> None:
    config = replace(
        _config(tmp_path, apply=True),
        preflight_required_env=("DATABASE_URL", "FMP_API_KEY", "LINEAR_API_KEY"),
        preflight_required_commands=("definitely-not-a-silver-command",),
    )

    result = objective_run.run_preflight(
        config,
        env={"PATH": os.environ.get("PATH", ""), "DATABASE_URL": "postgres://local"},
    )

    errors = [check.subject for check in result.checks if check.status == "error"]
    assert errors == [
        "environment FMP_API_KEY",
        "environment LINEAR_API_KEY",
        "command definitely-not-a-silver-command",
    ]
    assert objective_run.preflight_passed(result) is False


def test_preflight_runs_project_adapter_checks(tmp_path: Path) -> None:
    config = replace(
        _config(tmp_path, apply=True),
        preflight_project_checks=(
            "{python} -c 'import sys; sys.exit(0)'",
            "{python} -c 'print(\"FAIL: live DB is not reachable\"); "
            "import sys; sys.exit(1)'",
        ),
    )

    result = objective_run.run_preflight(config, env=os.environ)

    assert result.checks[-2].status == "ok"
    assert result.checks[-2].message == "passed"
    assert result.checks[-1].status == "error"
    assert result.checks[-1].message == "FAIL: live DB is not reachable"


def test_safety_dry_run_stops_before_dispatch(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger.db"
    _write_minimal_ledger(ledger, status="Ready")
    config = _config(tmp_path, ledger=ledger, apply=True)
    runner = _SafetyFindingRunner()
    observer = _FakeObserver(())

    result = objective_run.run_cycle(
        config,
        cycle=1,
        runner=runner,
        observer=observer,
    )

    assert result.stopped is True
    assert result.stop_reason == "safety dry-run found pre-dispatch blocker"
    assert [item.kind for item in result.command_results] == [
        "import",
        "admit",
        "safety",
    ]
    assert result.blockers == (
        "- obj-001 | move_safety_review | Ready -> Safety Review | planned stop",
    )


def test_final_proof_packet_is_written_for_apply_runs(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger.db"
    _write_minimal_ledger(ledger, status="Done")
    config = _config(tmp_path, ledger=ledger, apply=True)
    preflight = objective_run.PreflightResult(
        (objective_run.PreflightCheck("ok", "environment DATABASE_URL", "set"),)
    )
    result = objective_run.CycleResult(
        cycle=1,
        command_results=(),
        runner_actions=(),
        blockers=(),
        stopped=False,
        stop_reason=None,
    )

    path = objective_run.write_final_proof_packet(
        config=config,
        preflight=preflight,
        results=(result,),
    )

    assert path is not None
    body = path.read_text(encoding="utf-8")
    assert "# Objective Run Proof Packet" in body
    assert "obj: active | Test objective | Done=1" in body
    assert "`python -m pytest`" in body


def _config(
    tmp_path: Path,
    *,
    ledger: Path | None = None,
    apply: bool,
    repair_mode: str = "plan",
    push_repairs: bool = False,
    run_repair_validation: bool = False,
):
    return objective_run.ControllerConfig(
        root=ROOT,
        ledger=ledger or tmp_path / "ledger.db",
        config_path=tmp_path / "agentic_build.yaml",
        project="silver-test",
        team_id=None,
        repo="SilverEnv/Silver",
        limit=100,
        max_active=5,
        ready_buffer=5,
        apply=apply,
        repair_mode=repair_mode,
        push_repairs=push_repairs,
        run_repair_validation=run_repair_validation,
        repair_agent_command=None,
        poll_interval=60,
        max_cycles=1,
        watch=False,
        stop_on_safety=True,
        observe_runner=True,
        quiet=True,
        preflight_required_env=(),
        preflight_required_commands=(),
        preflight_required_auth=(),
        preflight_project_checks=(),
        proof_packet_dir=tmp_path / "proof_packets",
        validation_commands=("python -m pytest",),
        output_format="text",
    )


def _write_minimal_ledger(ledger: Path, *, status: str) -> None:
    objective_run.work_ledger.initialize_ledger(ledger)
    now = objective_run.work_ledger.utc_now()
    with sqlite3.connect(ledger) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(
            """
            INSERT INTO objectives (
              id, title, user_value, why_now, source_path, status,
              validation_json, conflict_zones_json, imported_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "obj",
                "Test objective",
                "test value",
                "now",
                "docs/objectives/active/obj.md",
                "active",
                "[]",
                "[]",
                now,
                now,
            ),
        )
        connection.execute(
            """
            INSERT INTO tickets (
              id, objective_id, sequence, title, purpose, objective_impact,
              technical_summary, ticket_role, dependency_group,
              contracts_touched_json, status, risk_class, conflict_domain,
              owns_json, do_not_touch_json, dependencies_json,
              conflict_zones_json, validation_json, proof_packet_json,
              created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "obj-001",
                "obj",
                1,
                "Test ticket",
                "purpose",
                "impact",
                "summary",
                "implementation",
                "default",
                "[]",
                status,
                "low",
                "general",
                "[]",
                "[]",
                "[]",
                "[]",
                "[]",
                "[]",
                now,
                now,
            ),
        )
        connection.commit()


class _FakeRunner:
    def __init__(self, *, fail_kind: str | None = None) -> None:
        self.fail_kind = fail_kind

    def run(self, command, *, cwd: Path):
        return objective_run.CommandResult(
            kind=command.kind,
            argv=command.argv,
            returncode=1 if command.kind == self.fail_kind else 0,
            stdout="",
            stderr="failed" if command.kind == self.fail_kind else "",
        )


class _SafetyFindingRunner:
    def run(self, command, *, cwd: Path):
        stdout = ""
        if command.kind == "safety":
            stdout = (
                "DRY RUN: VCS reconciliation result\n"
                "- obj-001 | move_safety_review | Ready -> Safety Review | "
                "planned stop\n"
            )
        return objective_run.CommandResult(
            kind=command.kind,
            argv=command.argv,
            returncode=0,
            stdout=stdout,
            stderr="",
        )


class _ClearingSafetyRunner:
    def __init__(self, ledger: Path) -> None:
        self.ledger = ledger

    def run(self, command, *, cwd: Path):
        if command.kind == "vcs":
            with objective_run.work_ledger.connect_existing(self.ledger) as connection:
                objective_run.work_ledger.transition_ticket(
                    connection,
                    ticket_id="obj-001",
                    status="Merging",
                    actor="test_vcs",
                    message="planned safety allowance cleared",
                )
        return objective_run.CommandResult(
            kind=command.kind,
            argv=command.argv,
            returncode=0,
            stdout="",
            stderr="",
        )


class _FakeObserver:
    def __init__(self, actions) -> None:
        self.actions = tuple(actions)

    def observe(self, config):
        return self.actions
