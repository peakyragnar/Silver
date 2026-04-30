#!/usr/bin/env python
"""Run bounded repair attempts for integration Rework PR branches."""

from __future__ import annotations

import argparse
import json
import re
import shlex
import shutil
import sqlite3
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import integration_steward  # noqa: E402
import work_ledger  # noqa: E402


ACTOR = "integration_repair_runner"
DEFAULT_WORKTREE_ROOT = ROOT / ".silver" / "integration-repairs"
REPAIRABLE_KINDS = frozenset(("merge_conflict", "failed_check", "general_rework"))

PlanAction = Literal["repair", "skip"]
RunStatus = Literal["planned", "skipped", "succeeded", "blocked", "failed"]
OutputFormat = Literal["text", "json"]


class IntegrationRepairRunnerError(RuntimeError):
    """Raised when the repair runner cannot safely continue."""


@dataclass(frozen=True, slots=True)
class CommandResult:
    command: str
    cwd: str
    returncode: int
    stdout: str
    stderr: str


@dataclass(frozen=True, slots=True)
class RepairRunPlan:
    ticket_id: str
    objective_id: str
    title: str
    repair_kind: str
    action: PlanAction
    reason: str
    pr_url: str | None
    branch: str | None
    worktree_path: Path
    validation: tuple[str, ...]
    packet_body: str


@dataclass(frozen=True, slots=True)
class RepairRunResult:
    plan: RepairRunPlan
    status: RunStatus
    reason: str
    commands: tuple[CommandResult, ...]
    conflicts: tuple[str, ...]
    pushed: bool
    transitioned: bool


@dataclass(frozen=True, slots=True)
class RunnerConfig:
    repo_root: Path
    worktree_root: Path
    remote: str
    base_branch: str
    apply: bool
    push: bool
    run_validation: bool
    agent_command: str | None


class CommandRunner:
    def run(
        self,
        command: Sequence[str],
        *,
        cwd: Path,
        check: bool = True,
    ) -> CommandResult:
        result = subprocess.run(
            list(command),
            cwd=cwd,
            text=True,
            capture_output=True,
            check=False,
        )
        command_result = CommandResult(
            command=shlex.join(command),
            cwd=str(cwd),
            returncode=result.returncode,
            stdout=result.stdout,
            stderr=result.stderr,
        )
        if check and result.returncode != 0:
            raise IntegrationRepairRunnerError(command_error(command_result))
        return command_result

    def run_shell(
        self,
        command: str,
        *,
        cwd: Path,
        check: bool = True,
    ) -> CommandResult:
        result = subprocess.run(
            command,
            cwd=cwd,
            text=True,
            capture_output=True,
            shell=True,
            check=False,
        )
        command_result = CommandResult(
            command=command,
            cwd=str(cwd),
            returncode=result.returncode,
            stdout=result.stdout,
            stderr=result.stderr,
        )
        if check and result.returncode != 0:
            raise IntegrationRepairRunnerError(command_error(command_result))
        return command_result


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--ledger",
        type=Path,
        default=work_ledger.default_ledger_path(),
        help="ledger path; defaults to SILVER_LEDGER_PATH or .silver/work_ledger.db",
    )
    parser.add_argument("--ticket-id", default="", help="repair one ledger ticket")
    parser.add_argument(
        "--worktree-root",
        type=Path,
        default=DEFAULT_WORKTREE_ROOT,
        help="root directory for isolated repair worktrees",
    )
    parser.add_argument("--remote", default="origin")
    parser.add_argument("--base-branch", default="main")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="create worktrees and run repair commands",
    )
    parser.add_argument(
        "--push",
        action="store_true",
        help="push repaired branches and move tickets back to Merging",
    )
    parser.add_argument(
        "--run-validation",
        action="store_true",
        help="run validation commands from the repair packet",
    )
    parser.add_argument(
        "--agent-command",
        default="",
        help=(
            "optional shell command for agentic repair; placeholders: "
            "{packet_file}, {worktree}, {ticket_id}, {branch}"
        ),
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="output format",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate local configuration without writes",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    config = RunnerConfig(
        repo_root=ROOT,
        worktree_root=args.worktree_root,
        remote=args.remote,
        base_branch=args.base_branch,
        apply=args.apply,
        push=args.push,
        run_validation=args.run_validation,
        agent_command=args.agent_command or None,
    )
    try:
        if args.check:
            print(check_configuration(args=args, config=config))
            return 0

        with work_ledger.connect_existing(args.ledger) as connection:
            packets = integration_steward.repair_packets(connection)
            if args.ticket_id:
                packets = tuple(
                    packet for packet in packets if packet.ticket_id == args.ticket_id
                )
            plans = repair_run_plans(packets, worktree_root=config.worktree_root)
            results = run_repair_plans(
                connection=connection,
                plans=plans,
                config=config,
                runner=CommandRunner(),
            )
        print(render_results(results, output_format=args.format))
    except (
        IntegrationRepairRunnerError,
        integration_steward.IntegrationStewardError,
        work_ledger.WorkLedgerError,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


def check_configuration(*, args: argparse.Namespace, config: RunnerConfig) -> str:
    if shutil.which("git") is None:
        raise IntegrationRepairRunnerError("git is required")
    if config.push and not config.apply:
        raise IntegrationRepairRunnerError("--push requires --apply")
    if config.run_validation and not config.apply:
        raise IntegrationRepairRunnerError("--run-validation requires --apply")
    with work_ledger.connect_existing(args.ledger):
        pass
    return "\n".join(
        (
            "Integration repair runner configuration check",
            "",
            f"Ledger: {args.ledger}",
            f"Worktree root: {config.worktree_root}",
            f"Remote/base: {config.remote}/{config.base_branch}",
            "Result: local integration repair runner configuration is valid",
        )
    )


def repair_run_plans(
    packets: Sequence[integration_steward.RepairPacket],
    *,
    worktree_root: Path,
) -> tuple[RepairRunPlan, ...]:
    return tuple(repair_run_plan(packet, worktree_root=worktree_root) for packet in packets)


def repair_run_plan(
    packet: integration_steward.RepairPacket,
    *,
    worktree_root: Path,
) -> RepairRunPlan:
    action: PlanAction = "repair"
    reason = "repair branch can be prepared"
    if not packet.pr_url or not packet.branch:
        action = "skip"
        reason = "missing PR URL or branch evidence"
    elif packet.repair_kind not in REPAIRABLE_KINDS:
        action = "skip"
        reason = f"repair kind is not automatically repairable: {packet.repair_kind}"
    elif not safe_branch_name(packet.branch):
        action = "skip"
        reason = f"unsafe branch name: {packet.branch}"

    return RepairRunPlan(
        ticket_id=packet.ticket_id,
        objective_id=packet.objective_id,
        title=packet.title,
        repair_kind=packet.repair_kind,
        action=action,
        reason=reason,
        pr_url=packet.pr_url,
        branch=packet.branch,
        worktree_path=worktree_root / sanitize_path_part(packet.ticket_id),
        validation=packet.validation,
        packet_body=packet.body,
    )


def safe_branch_name(branch: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9._/-]+", branch)) and ".." not in branch


def sanitize_path_part(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip(".-")
    if not sanitized:
        raise IntegrationRepairRunnerError(f"cannot sanitize path part: {value!r}")
    return sanitized


def run_repair_plans(
    *,
    connection: sqlite3.Connection,
    plans: Sequence[RepairRunPlan],
    config: RunnerConfig,
    runner: CommandRunner,
) -> tuple[RepairRunResult, ...]:
    if config.push and not config.apply:
        raise IntegrationRepairRunnerError("--push requires --apply")
    if config.run_validation and not config.apply:
        raise IntegrationRepairRunnerError("--run-validation requires --apply")

    results: list[RepairRunResult] = []
    for plan in plans:
        if plan.action == "skip":
            result = RepairRunResult(
                plan=plan,
                status="skipped",
                reason=plan.reason,
                commands=(),
                conflicts=(),
                pushed=False,
                transitioned=False,
            )
        elif not config.apply:
            result = RepairRunResult(
                plan=plan,
                status="planned",
                reason=plan.reason,
                commands=(),
                conflicts=(),
                pushed=False,
                transitioned=False,
            )
        else:
            result = execute_repair_plan(plan, config=config, runner=runner)
            record_repair_result(connection, result)
        results.append(result)
    return tuple(results)


def execute_repair_plan(
    plan: RepairRunPlan,
    *,
    config: RunnerConfig,
    runner: CommandRunner,
) -> RepairRunResult:
    commands: list[CommandResult] = []
    conflicts: tuple[str, ...] = ()
    if plan.branch is None:
        return blocked_result(plan, "missing branch", commands, conflicts)
    if plan.worktree_path.exists():
        return blocked_result(
            plan,
            f"repair worktree already exists: {plan.worktree_path}",
            commands,
            conflicts,
        )

    plan.worktree_path.parent.mkdir(parents=True, exist_ok=True)
    commands.append(
        runner.run(
            ["git", "fetch", config.remote, config.base_branch, plan.branch],
            cwd=config.repo_root,
        )
    )
    commands.append(
        runner.run(
            [
                "git",
                "worktree",
                "add",
                "-B",
                plan.branch,
                str(plan.worktree_path),
                f"{config.remote}/{plan.branch}",
            ],
            cwd=config.repo_root,
        )
    )

    merge_result = runner.run(
        ["git", "merge", "--no-edit", f"{config.remote}/{config.base_branch}"],
        cwd=plan.worktree_path,
        check=False,
    )
    commands.append(merge_result)
    if merge_result.returncode != 0:
        conflicts_result = runner.run(
            ["git", "diff", "--name-only", "--diff-filter=U"],
            cwd=plan.worktree_path,
            check=False,
        )
        commands.append(conflicts_result)
        conflicts = tuple(
            line.strip()
            for line in conflicts_result.stdout.splitlines()
            if line.strip()
        )
        if config.agent_command is None:
            commands.append(
                runner.run(
                    ["git", "merge", "--abort"],
                    cwd=plan.worktree_path,
                    check=False,
                )
            )
            return blocked_result(
                plan,
                "merge produced conflicts and no agent command was configured",
                commands,
                conflicts,
            )

    if config.agent_command is not None:
        packet_file = write_packet_file(plan)
        agent_command = render_agent_command(
            config.agent_command,
            plan=plan,
            packet_file=packet_file,
        )
        agent_result = runner.run_shell(agent_command, cwd=plan.worktree_path, check=False)
        commands.append(agent_result)
        if agent_result.returncode != 0:
            return failed_result(plan, "agent command failed", commands, conflicts)
        unresolved_result = runner.run(
            ["git", "diff", "--name-only", "--diff-filter=U"],
            cwd=plan.worktree_path,
            check=False,
        )
        commands.append(unresolved_result)
        unresolved = tuple(
            line.strip()
            for line in unresolved_result.stdout.splitlines()
            if line.strip()
        )
        if unresolved:
            return blocked_result(
                plan,
                "agent command left unresolved conflicts",
                commands,
                unresolved,
            )

    if config.run_validation:
        for validation in plan.validation:
            if not validation or validation == "none":
                continue
            validation_result = runner.run_shell(
                validation,
                cwd=plan.worktree_path,
                check=False,
            )
            commands.append(validation_result)
            if validation_result.returncode != 0:
                return failed_result(
                    plan,
                    f"validation failed: {validation}",
                    commands,
                    conflicts,
                )

    commit_result = commit_if_needed(plan, runner=runner, commands=commands)
    commands.extend(commit_result)

    pushed = False
    transitioned = False
    if config.push:
        commands.append(
            runner.run(
                ["git", "push", config.remote, f"HEAD:{plan.branch}"],
                cwd=plan.worktree_path,
            )
        )
        pushed = True
        transitioned = True

    return RepairRunResult(
        plan=plan,
        status="succeeded",
        reason="repair commands completed" + (" and branch was pushed" if pushed else ""),
        commands=tuple(commands),
        conflicts=conflicts,
        pushed=pushed,
        transitioned=transitioned,
    )


def write_packet_file(plan: RepairRunPlan) -> Path:
    packet_dir = plan.worktree_path / ".silver"
    packet_dir.mkdir(parents=True, exist_ok=True)
    packet_file = packet_dir / "integration-repair-packet.md"
    packet_file.write_text(plan.packet_body + "\n", encoding="utf-8")
    return packet_file


def render_agent_command(
    template: str,
    *,
    plan: RepairRunPlan,
    packet_file: Path,
) -> str:
    replacements = {
        "packet_file": shlex.quote(str(packet_file)),
        "worktree": shlex.quote(str(plan.worktree_path)),
        "ticket_id": shlex.quote(plan.ticket_id),
        "branch": shlex.quote(plan.branch or ""),
    }
    try:
        return template.format(**replacements)
    except KeyError as exc:
        raise IntegrationRepairRunnerError(
            f"unknown agent command placeholder: {exc}"
        ) from exc


def commit_if_needed(
    plan: RepairRunPlan,
    *,
    runner: CommandRunner,
    commands: Sequence[CommandResult],
) -> tuple[CommandResult, ...]:
    pending_merge = any(
        result.command.startswith("git merge ") and result.returncode != 0
        for result in commands
    )
    status = runner.run(
        ["git", "status", "--porcelain"],
        cwd=plan.worktree_path,
        check=False,
    )
    if not status.stdout.strip():
        return (status,)
    add = runner.run(["git", "add", "-A"], cwd=plan.worktree_path)
    if pending_merge:
        commit = runner.run(["git", "commit", "--no-edit"], cwd=plan.worktree_path)
    else:
        commit = runner.run(
            ["git", "commit", "-m", f"Repair {plan.ticket_id}: {plan.repair_kind}"],
            cwd=plan.worktree_path,
        )
    return (status, add, commit)


def blocked_result(
    plan: RepairRunPlan,
    reason: str,
    commands: Sequence[CommandResult],
    conflicts: Sequence[str],
) -> RepairRunResult:
    return RepairRunResult(
        plan=plan,
        status="blocked",
        reason=reason,
        commands=tuple(commands),
        conflicts=tuple(conflicts),
        pushed=False,
        transitioned=False,
    )


def failed_result(
    plan: RepairRunPlan,
    reason: str,
    commands: Sequence[CommandResult],
    conflicts: Sequence[str],
) -> RepairRunResult:
    return RepairRunResult(
        plan=plan,
        status="failed",
        reason=reason,
        commands=tuple(commands),
        conflicts=tuple(conflicts),
        pushed=False,
        transitioned=False,
    )


def record_repair_result(
    connection: sqlite3.Connection,
    result: RepairRunResult,
) -> None:
    message = repair_result_message(result)
    if result.transitioned:
        work_ledger.transition_ticket(
            connection,
            ticket_id=result.plan.ticket_id,
            status="Merging",
            actor=ACTOR,
            message=message,
        )
        return

    now = work_ledger.utc_now()
    with connection:
        work_ledger.insert_event(
            connection,
            ticket_id=result.plan.ticket_id,
            objective_id=result.plan.objective_id,
            event_type=f"integration_repair_{result.status}",
            from_status=work_ledger.TICKET_STATUS_REWORK,
            to_status=work_ledger.TICKET_STATUS_REWORK,
            message=message,
            actor=ACTOR,
            created_at=now,
        )


def repair_result_message(result: RepairRunResult) -> str:
    lines = [
        f"Integration repair runner {result.status}: {result.reason}",
        f"PR: {result.plan.pr_url or 'missing'}",
        f"Branch: {result.plan.branch or 'missing'}",
        f"Worktree: {result.plan.worktree_path}",
    ]
    if result.conflicts:
        lines.append("Conflicts:")
        lines.extend(f"- {path}" for path in result.conflicts)
    if result.commands:
        lines.append("Commands:")
        lines.extend(
            f"- {command.command} -> {command.returncode}"
            for command in result.commands
        )
    return "\n".join(lines)


def command_error(result: CommandResult) -> str:
    detail = result.stderr.strip() or result.stdout.strip()
    return f"command failed ({result.command}) in {result.cwd}: {detail}"


def render_results(
    results: Sequence[RepairRunResult],
    *,
    output_format: OutputFormat,
) -> str:
    payload = {"results": [result_payload(result) for result in results]}
    if output_format == "json":
        return json.dumps(payload, indent=2, sort_keys=True)
    if not results:
        return "No repair packets to run."
    lines = ["Integration repair runner results:"]
    for result in results:
        lines.append(
            f"- {result.plan.ticket_id} | {result.status} | "
            f"{result.plan.repair_kind} | {result.reason}"
        )
    return "\n".join(lines)


def result_payload(result: RepairRunResult) -> Mapping[str, object]:
    return {
        "ticket_id": result.plan.ticket_id,
        "objective_id": result.plan.objective_id,
        "repair_kind": result.plan.repair_kind,
        "status": result.status,
        "reason": result.reason,
        "pr_url": result.plan.pr_url,
        "branch": result.plan.branch,
        "worktree_path": str(result.plan.worktree_path),
        "conflicts": list(result.conflicts),
        "pushed": result.pushed,
        "transitioned": result.transitioned,
        "commands": [
            {
                "command": command.command,
                "cwd": command.cwd,
                "returncode": command.returncode,
            }
            for command in result.commands
        ],
    }


if __name__ == "__main__":
    raise SystemExit(main())
