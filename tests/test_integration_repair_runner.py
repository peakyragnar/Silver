from __future__ import annotations

import importlib.util
import shlex
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INTEGRATION_REPAIR_RUNNER_SCRIPT = ROOT / "scripts" / "integration_repair_runner.py"


def load_integration_repair_runner_module():
    spec = importlib.util.spec_from_file_location(
        "integration_repair_runner",
        INTEGRATION_REPAIR_RUNNER_SCRIPT,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


integration_repair_runner = load_integration_repair_runner_module()
integration_steward = integration_repair_runner.integration_steward
work_ledger = integration_repair_runner.work_ledger


def test_plan_skips_missing_pr_evidence(tmp_path: Path) -> None:
    packet = _packet(pr_url=None, branch=None)

    plan = integration_repair_runner.repair_run_plan(
        packet,
        worktree_root=tmp_path / "worktrees",
    )

    assert plan.action == "skip"
    assert "missing PR" in plan.reason


def test_dry_run_plans_repair_without_commands(tmp_path: Path) -> None:
    connection = _ledger_connection(tmp_path)
    _insert_rework_ticket(connection)
    plans = integration_repair_runner.repair_run_plans(
        integration_steward.repair_packets(connection),
        worktree_root=tmp_path / "worktrees",
    )

    results = integration_repair_runner.run_repair_plans(
        connection=connection,
        plans=plans,
        config=_config(tmp_path, apply=False),
        runner=_FakeRunner(),
    )

    assert [result.status for result in results] == ["planned"]
    assert results[0].commands == ()


def test_clean_branch_update_pushes_and_moves_ticket_to_merging(
    tmp_path: Path,
) -> None:
    connection = _ledger_connection(tmp_path)
    _insert_rework_ticket(connection)
    runner = _FakeRunner()
    plans = integration_repair_runner.repair_run_plans(
        integration_steward.repair_packets(connection),
        worktree_root=tmp_path / "worktrees",
    )

    results = integration_repair_runner.run_repair_plans(
        connection=connection,
        plans=plans,
        config=_config(tmp_path, apply=True, push=True),
        runner=runner,
    )

    ticket = work_ledger.select_ticket(connection, "portable-orchestration-core-001")

    assert [result.status for result in results] == ["succeeded"]
    assert results[0].pushed is True
    assert results[0].transitioned is True
    assert ticket.status == "Merging"
    assert any("git push origin HEAD:arr-61-repair" in command for command in runner.commands)


def test_merge_conflict_without_agent_blocks_and_keeps_rework(
    tmp_path: Path,
) -> None:
    connection = _ledger_connection(tmp_path)
    _insert_rework_ticket(connection)
    runner = _FakeRunner(merge_returncode=1, conflict_stdout="docs/conflict.md\n")
    plans = integration_repair_runner.repair_run_plans(
        integration_steward.repair_packets(connection),
        worktree_root=tmp_path / "worktrees",
    )

    results = integration_repair_runner.run_repair_plans(
        connection=connection,
        plans=plans,
        config=_config(tmp_path, apply=True, push=True),
        runner=runner,
    )
    ticket = work_ledger.select_ticket(connection, "portable-orchestration-core-001")
    latest = work_ledger.recent_events(
        connection,
        ticket_id="portable-orchestration-core-001",
        limit=1,
    )[0]

    assert [result.status for result in results] == ["blocked"]
    assert results[0].conflicts == ("docs/conflict.md",)
    assert ticket.status == "Rework"
    assert latest["event_type"] == "integration_repair_blocked"
    assert any("git merge --abort" in command for command in runner.commands)
    assert not any("git push" in command for command in runner.commands)


def test_agent_repair_can_commit_pending_merge_and_push(tmp_path: Path) -> None:
    connection = _ledger_connection(tmp_path)
    _insert_rework_ticket(connection)
    runner = _FakeRunner(
        merge_returncode=1,
        conflict_stdout="docs/conflict.md\n",
        post_agent_conflict_stdout="",
        status_stdout=" M docs/conflict.md\n",
    )
    plans = integration_repair_runner.repair_run_plans(
        integration_steward.repair_packets(connection),
        worktree_root=tmp_path / "worktrees",
    )

    results = integration_repair_runner.run_repair_plans(
        connection=connection,
        plans=plans,
        config=_config(
            tmp_path,
            apply=True,
            push=True,
            agent_command="repair-agent --packet {packet_file} --branch {branch}",
        ),
        runner=runner,
    )

    assert [result.status for result in results] == ["succeeded"]
    assert any("repair-agent --packet" in command for command in runner.commands)
    assert any(command == "git commit --no-edit" for command in runner.commands)
    assert any("git push origin HEAD:arr-61-repair" in command for command in runner.commands)


def test_validation_failure_blocks_push_and_keeps_rework(tmp_path: Path) -> None:
    connection = _ledger_connection(tmp_path)
    _insert_rework_ticket(connection)
    runner = _FakeRunner(validation_returncode=1)
    plans = integration_repair_runner.repair_run_plans(
        integration_steward.repair_packets(connection),
        worktree_root=tmp_path / "worktrees",
    )

    results = integration_repair_runner.run_repair_plans(
        connection=connection,
        plans=plans,
        config=_config(tmp_path, apply=True, push=True, run_validation=True),
        runner=runner,
    )
    ticket = work_ledger.select_ticket(connection, "portable-orchestration-core-001")
    latest = work_ledger.recent_events(
        connection,
        ticket_id="portable-orchestration-core-001",
        limit=1,
    )[0]

    assert [result.status for result in results] == ["failed"]
    assert "validation failed" in results[0].reason
    assert ticket.status == "Rework"
    assert latest["event_type"] == "integration_repair_failed"
    assert not any("git push" in command for command in runner.commands)


def _config(
    tmp_path: Path,
    *,
    apply: bool,
    push: bool = False,
    run_validation: bool = False,
    agent_command: str | None = None,
):
    repo_root = tmp_path / "repo"
    repo_root.mkdir(exist_ok=True)
    return integration_repair_runner.RunnerConfig(
        repo_root=repo_root,
        worktree_root=tmp_path / "worktrees",
        remote="origin",
        base_branch="main",
        apply=apply,
        push=push,
        run_validation=run_validation,
        agent_command=agent_command,
    )


def _packet(
    *,
    pr_url: str | None = "https://github.com/SilverEnv/Silver/pull/61",
    branch: str | None = "arr-61-repair",
    repair_kind: str = "merge_conflict",
):
    return integration_steward.RepairPacket(
        ticket_id="portable-orchestration-core-001",
        objective_id="portable-orchestration-core",
        title="Repair PR",
        repair_kind=repair_kind,
        blocker="PR has merge conflicts",
        pr_url=pr_url,
        branch=branch,
        owns=("scripts/integration_repair_runner.py",),
        do_not_touch=("db/migrations/",),
        validation=("python -m pytest",),
        proof_packet=("validation output",),
        body="## Integration Repair Packet\n",
    )


def _ledger_connection(tmp_path: Path):
    ledger_path = tmp_path / "ledger.db"
    work_ledger.initialize_ledger(ledger_path)
    return work_ledger.connect_existing(ledger_path)


def _insert_rework_ticket(connection) -> None:
    now = work_ledger.utc_now()
    objective_id = "portable-orchestration-core"
    ticket_id = "portable-orchestration-core-001"
    with connection:
        connection.execute(
            """
            INSERT OR IGNORE INTO objectives (
              id, title, user_value, why_now, source_path, status,
              validation_json, conflict_zones_json, imported_at, updated_at
            )
            VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, ?)
            """,
            (
                objective_id,
                "Build the portable objective-to-ticket orchestration core",
                "Michael can run objective-aware repair lanes.",
                "Rework needs executable repair.",
                work_ledger.OBJECTIVE_STATUS_ACTIVE,
                work_ledger.dumps_json(("git diff --check",)),
                work_ledger.dumps_json(("scripts/",)),
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
              branch, pr_url, linear_identifier, created_at, updated_at
            )
            VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?, 'Rework', ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ticket_id,
                objective_id,
                "Run automatic integration repair",
                "Make Rework executable.",
                "Routine repair can be attempted without Michael.",
                "Prepare worktree, update branch, validate, and push.",
                "integration",
                "orchestration-core",
                work_ledger.dumps_json(("objective-dag",)),
                "low",
                "orchestration",
                work_ledger.dumps_json(("scripts/integration_repair_runner.py",)),
                work_ledger.dumps_json(("db/migrations/",)),
                work_ledger.dumps_json(()),
                work_ledger.dumps_json(("scripts/",)),
                work_ledger.dumps_json(("python -m pytest",)),
                work_ledger.dumps_json(("validation output",)),
                "arr-61-repair",
                "https://github.com/SilverEnv/Silver/pull/61",
                "ARR-61",
                now,
                now,
            ),
        )
        work_ledger.insert_event(
            connection,
            ticket_id=ticket_id,
            objective_id=objective_id,
            event_type="status_transition",
            from_status="Merging",
            to_status="Rework",
            message="PR #61 https://github.com/SilverEnv/Silver/pull/61: PR has merge conflicts",
            actor="vcs_reconciler",
            created_at=now,
        )


class _FakeRunner:
    def __init__(
        self,
        *,
        merge_returncode: int = 0,
        conflict_stdout: str = "",
        post_agent_conflict_stdout: str | None = None,
        status_stdout: str = "",
        validation_returncode: int = 0,
    ) -> None:
        self.merge_returncode = merge_returncode
        self.conflict_stdout = conflict_stdout
        self.post_agent_conflict_stdout = post_agent_conflict_stdout
        self.status_stdout = status_stdout
        self.validation_returncode = validation_returncode
        self.commands: list[str] = []
        self.diff_calls = 0

    def run(self, command, *, cwd: Path, check: bool = True):
        command_text = shlex.join(command)
        self.commands.append(command_text)
        returncode = 0
        stdout = ""
        stderr = ""

        if command[:3] == ["git", "worktree", "add"]:
            Path(command[5]).mkdir(parents=True, exist_ok=True)
        elif command[:2] == ["git", "merge"] and command[2] != "--abort":
            returncode = self.merge_returncode
            stderr = "merge conflict" if returncode else ""
        elif command[:4] == ["git", "diff", "--name-only", "--diff-filter=U"]:
            self.diff_calls += 1
            if self.diff_calls == 1 or self.post_agent_conflict_stdout is None:
                stdout = self.conflict_stdout
            else:
                stdout = self.post_agent_conflict_stdout
        elif command[:3] == ["git", "status", "--porcelain"]:
            stdout = self.status_stdout

        result = integration_repair_runner.CommandResult(
            command=command_text,
            cwd=str(cwd),
            returncode=returncode,
            stdout=stdout,
            stderr=stderr,
        )
        if check and returncode != 0:
            raise AssertionError(f"unexpected checked failure: {command_text}")
        return result

    def run_shell(self, command: str, *, cwd: Path, check: bool = True):
        self.commands.append(command)
        returncode = 0
        stderr = ""
        if command.startswith("python -m pytest"):
            returncode = self.validation_returncode
            stderr = "validation failed" if returncode else ""
        result = integration_repair_runner.CommandResult(
            command=command,
            cwd=str(cwd),
            returncode=returncode,
            stdout="",
            stderr=stderr,
        )
        if check and returncode != 0:
            raise AssertionError(f"unexpected checked shell failure: {command}")
        return result
