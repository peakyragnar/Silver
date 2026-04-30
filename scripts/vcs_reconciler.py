#!/usr/bin/env python
"""Reconcile GitHub PR state back into the local Objective work ledger."""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sqlite3
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import merge_steward  # noqa: E402
import work_ledger  # noqa: E402


DEFAULT_LIMIT = 100
ACTOR = "vcs_reconciler"

ReconciliationActionName = Literal[
    "mark_done",
    "move_merging",
    "move_rework",
    "move_safety_review",
    "wait",
    "skip",
]
OutputFormat = Literal["text", "json"]


class VcsReconcilerError(RuntimeError):
    """Raised when VCS reconciliation cannot safely continue."""


@dataclass(frozen=True, slots=True)
class TicketRecord:
    id: str
    objective_id: str
    sequence: int
    title: str
    status: str
    ticket_role: str
    dependency_group: str
    contracts_touched: tuple[str, ...]
    risk_class: str
    objective_impact: str
    conflict_domain: str
    owns: tuple[str, ...]
    do_not_touch: tuple[str, ...]
    validation: tuple[str, ...]
    proof_packet: tuple[str, ...]
    branch: str | None
    pr_url: str | None
    linear_identifier: str | None


@dataclass(frozen=True, slots=True)
class ReconciliationAction:
    ticket_id: str
    objective_id: str
    from_status: str
    action: ReconciliationActionName
    target_status: str | None
    reason: str
    pr_number: int | None
    pr_url: str | None
    branch: str | None


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--ledger",
        type=Path,
        default=work_ledger.default_ledger_path(),
        help="ledger path; defaults to SILVER_LEDGER_PATH or .silver/work_ledger.db",
    )
    parser.add_argument(
        "--repo",
        default=merge_steward.detect_github_repo(),
        help="GitHub repository as owner/name; defaults to origin",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help="number of recent GitHub PRs to inspect",
    )
    parser.add_argument(
        "--required-check",
        action="append",
        dest="required_checks",
        help=(
            "required PR check name; may be repeated. Defaults to "
            "'Python 3.10 checks'."
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="write safe ledger state transitions and PR evidence",
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
        help="validate local configuration without network calls or writes",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    required_checks = tuple(args.required_checks or merge_steward.DEFAULT_REQUIRED_CHECKS)

    try:
        if args.check:
            print(check_configuration(args=args, required_checks=required_checks))
            return 0

        if not args.repo:
            raise VcsReconcilerError("GitHub repo is not configured")

        github = merge_steward.GitHubClient(args.repo)
        pull_requests = github.list_pull_requests(args.limit)
        pull_requests = with_matching_diffs(
            ledger_path=args.ledger,
            github=github,
            pull_requests=pull_requests,
        )
        with work_ledger.connect_existing(args.ledger) as connection:
            actions = reconcile_prs(
                connection,
                pull_requests,
                required_checks=required_checks,
                apply=args.apply,
            )
        print(render_actions(actions, output_format=args.format, applied=args.apply))
    except (
        VcsReconcilerError,
        work_ledger.WorkLedgerError,
        merge_steward.MergeStewardError,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    return 0


def check_configuration(
    *,
    args: argparse.Namespace,
    required_checks: Sequence[str],
) -> str:
    if not args.repo:
        raise VcsReconcilerError("GitHub repo is not configured")
    if shutil.which("gh") is None:
        raise VcsReconcilerError("GitHub CLI `gh` is required")
    with work_ledger.connect_existing(args.ledger):
        pass

    return "\n".join(
        (
            "VCS reconciler configuration check",
            "",
            f"Ledger: {args.ledger}",
            f"GitHub repo: {args.repo}",
            "Required checks: " + ", ".join(required_checks),
            "Result: local VCS reconciliation configuration is valid",
        )
    )


def with_matching_diffs(
    *,
    ledger_path: Path,
    github: merge_steward.GitHubClient,
    pull_requests: Sequence[merge_steward.PullRequest],
) -> tuple[merge_steward.PullRequest, ...]:
    with work_ledger.connect_existing(ledger_path) as connection:
        tickets = reconciliation_tickets(connection)

    matched_open_numbers = {
        pr.number
        for ticket in tickets
        for pr in [choose_pr_for_ticket(ticket, pull_requests)]
        if pr is not None and pr.state == "OPEN"
    }
    return tuple(
        github.with_diff(pr) if pr.number in matched_open_numbers else pr
        for pr in pull_requests
    )


def reconcile_prs(
    connection: sqlite3.Connection,
    pull_requests: Sequence[merge_steward.PullRequest],
    *,
    required_checks: Sequence[str],
    apply: bool,
) -> tuple[ReconciliationAction, ...]:
    actions: list[ReconciliationAction] = []
    for ticket in reconciliation_tickets(connection):
        pr = choose_pr_for_ticket(ticket, pull_requests)
        if pr is None:
            continue

        action = classify_pr(ticket, pr, required_checks)
        actions.append(action)
        if apply:
            apply_reconciliation_action(connection, action)
    return tuple(actions)


def reconciliation_tickets(connection: sqlite3.Connection) -> tuple[TicketRecord, ...]:
    terminal_statuses = tuple(work_ledger.TICKET_TERMINAL_STATUSES)
    rows = connection.execute(
        f"""
        SELECT * FROM tickets
        WHERE status NOT IN ({work_ledger.placeholders(terminal_statuses)})
        ORDER BY objective_id, sequence
        """,
        terminal_statuses,
    ).fetchall()
    return tuple(row_to_ticket_record(row) for row in rows)


def row_to_ticket_record(row: sqlite3.Row) -> TicketRecord:
    return TicketRecord(
        id=row["id"],
        objective_id=row["objective_id"],
        sequence=int(row["sequence"]),
        title=row["title"],
        status=row["status"],
        ticket_role=row["ticket_role"],
        dependency_group=row["dependency_group"],
        contracts_touched=tuple(work_ledger.loads_json(row["contracts_touched_json"])),
        risk_class=row["risk_class"],
        objective_impact=row["objective_impact"],
        conflict_domain=row["conflict_domain"],
        owns=tuple(work_ledger.loads_json(row["owns_json"])),
        do_not_touch=tuple(work_ledger.loads_json(row["do_not_touch_json"])),
        validation=tuple(work_ledger.loads_json(row["validation_json"])),
        proof_packet=tuple(work_ledger.loads_json(row["proof_packet_json"])),
        branch=row["branch"],
        pr_url=row["pr_url"],
        linear_identifier=row["linear_identifier"],
    )


def choose_pr_for_ticket(
    ticket: TicketRecord,
    pull_requests: Sequence[merge_steward.PullRequest],
) -> merge_steward.PullRequest | None:
    explicit_matches = [
        pr
        for pr in pull_requests
        if explicit_ticket_id(pr) == ticket.id.lower()
    ]
    if explicit_matches:
        return preferred_pr(explicit_matches)

    identity_matches = [
        pr
        for pr in pull_requests
        if pr_identity_matches_ticket(ticket, pr)
    ]
    if identity_matches:
        return preferred_pr(identity_matches)
    return None


def explicit_ticket_id(pr: merge_steward.PullRequest) -> str | None:
    for pattern in (
        r"(?im)^Ledger Ticket:\s*`?(?P<ticket>[a-z0-9][a-z0-9_-]*)`?\s*$",
        r"(?im)^ticket_id:\s*`?(?P<ticket>[a-z0-9][a-z0-9_-]*)`?\s*$",
    ):
        match = re.search(pattern, pr.body)
        if match is not None:
            return match.group("ticket").lower()
    return None


def pr_identity_matches_ticket(
    ticket: TicketRecord,
    pr: merge_steward.PullRequest,
) -> bool:
    identity_text = "\n".join((pr.title, pr.head_ref_name))
    tokens = tuple(
        token
        for token in (ticket.linear_identifier, ticket.id)
        if token is not None and token
    )
    return any(contains_identity_token(identity_text, token) for token in tokens)


def contains_identity_token(text: str, token: str) -> bool:
    escaped = re.escape(token)
    return bool(re.search(rf"(?<![a-z0-9]){escaped}(?![a-z0-9])", text, re.I))


def preferred_pr(
    pull_requests: Sequence[merge_steward.PullRequest],
) -> merge_steward.PullRequest:
    open_matches = [pr for pr in pull_requests if pr.state == "OPEN"]
    candidates = open_matches or list(pull_requests)
    return max(candidates, key=lambda pr: pr.number)


def classify_pr(
    ticket: TicketRecord,
    pr: merge_steward.PullRequest,
    required_checks: Sequence[str],
) -> ReconciliationAction:
    if pr.state == "CLOSED" and not pr.merged_at:
        return build_action(
            ticket,
            pr,
            action="move_rework",
            target_status=work_ledger.TICKET_STATUS_REWORK,
            reason="PR is closed without merge",
        )

    if ticket.status == "Safety Review" and pr.state != "MERGED" and not pr.merged_at:
        issue = linear_issue_for_ticket(ticket)
        safety_trigger = merge_steward.safety_review_trigger(issue, pr)
        allowance = (
            objective_contract_allowance(ticket, pr, safety_trigger)
            if safety_trigger is not None
            else None
        )
        if allowance is None:
            return build_action(
                ticket,
                pr,
                action="wait",
                target_status=None,
                reason="ticket is already in Safety Review",
            )
        return classify_allowed_contract_pr(
            ticket,
            pr,
            required_checks,
            allowance_reason=allowance,
        )
    if ticket.status == "Blocked" and pr.state != "MERGED" and not pr.merged_at:
        return build_action(
            ticket,
            pr,
            action="wait",
            target_status=None,
            reason="ticket is Blocked; VCS reconciliation will not unblock it",
        )

    issue = linear_issue_for_ticket(ticket)
    decision = merge_steward.decide_issue_action(issue, pr, required_checks)

    if decision.action == "mark_done":
        return build_action(
            ticket,
            pr,
            action="mark_done",
            target_status="Done",
            reason=decision.reason,
        )
    if decision.action == "move_safety_review":
        allowance = objective_contract_allowance(ticket, pr, decision.reason)
        if allowance is not None:
            return classify_allowed_contract_pr(
                ticket,
                pr,
                required_checks,
                allowance_reason=allowance,
            )
        return build_action(
            ticket,
            pr,
            action="move_safety_review",
            target_status="Safety Review",
            reason=decision.reason,
        )
    if decision.action == "move_rework":
        return build_action(
            ticket,
            pr,
            action="move_rework",
            target_status=work_ledger.TICKET_STATUS_REWORK,
            reason=decision.reason,
        )
    if decision.action == "queue":
        return build_action(
            ticket,
            pr,
            action="move_merging",
            target_status="Merging",
            reason="PR is green and mergeable; ledger can hand it to merge steward",
        )
    if decision.action == "wait" and (
        pr.in_merge_queue or pr.auto_merge_enabled
    ):
        return build_action(
            ticket,
            pr,
            action="move_merging",
            target_status="Merging",
            reason=decision.reason,
        )
    if decision.action == "wait":
        return build_action(
            ticket,
            pr,
            action="wait",
            target_status=None,
            reason=decision.reason,
        )
    return build_action(
        ticket,
        pr,
        action="skip",
        target_status=None,
        reason=decision.reason,
    )


def classify_allowed_contract_pr(
    ticket: TicketRecord,
    pr: merge_steward.PullRequest,
    required_checks: Sequence[str],
    *,
    allowance_reason: str,
) -> ReconciliationAction:
    merge_state = (pr.merge_state_status or "UNKNOWN").upper()
    if merge_state == "DIRTY":
        return build_action(
            ticket,
            pr,
            action="move_rework",
            target_status=work_ledger.TICKET_STATUS_REWORK,
            reason=f"{allowance_reason}; PR has merge conflicts",
        )

    check_status = merge_steward.required_check_status(pr, required_checks)
    if check_status.action == "move_rework":
        return build_action(
            ticket,
            pr,
            action="move_rework",
            target_status=work_ledger.TICKET_STATUS_REWORK,
            reason=f"{allowance_reason}; {check_status.reason}",
        )
    if check_status.action == "wait":
        return build_action(
            ticket,
            pr,
            action="wait",
            target_status=None,
            reason=f"{allowance_reason}; {check_status.reason}",
        )

    if merge_state in {"BLOCKED", "UNKNOWN"}:
        return build_action(
            ticket,
            pr,
            action="wait",
            target_status=None,
            reason=f"{allowance_reason}; PR merge state is {merge_state}",
        )
    if pr.in_merge_queue:
        return build_action(
            ticket,
            pr,
            action="move_merging",
            target_status="Merging",
            reason=f"{allowance_reason}; PR is already in GitHub merge queue",
        )
    if pr.auto_merge_enabled:
        return build_action(
            ticket,
            pr,
            action="move_merging",
            target_status="Merging",
            reason=f"{allowance_reason}; PR auto-merge is already enabled",
        )

    return build_action(
        ticket,
        pr,
        action="move_merging",
        target_status="Merging",
        reason=f"{allowance_reason}; PR is green and mergeable",
    )


def objective_contract_allowance(
    ticket: TicketRecord,
    pr: merge_steward.PullRequest,
    safety_reason: str,
) -> str | None:
    issue = linear_issue_for_ticket(ticket)
    return merge_steward.planned_contract_safety_allowance(issue, pr, safety_reason)


def linear_issue_for_ticket(ticket: TicketRecord) -> merge_steward.LinearIssue:
    return merge_steward.LinearIssue(
        id=ticket.id,
        identifier=ticket.linear_identifier or ticket.id,
        title=ticket.title,
        url="",
        description=ticket_description_for_safety(ticket),
        state=ticket.status,
        team_states={},
    )


def ticket_description_for_safety(ticket: TicketRecord) -> str:
    lines = [
        f"Parent Objective: {ticket.objective_id}",
        f"Ledger Ticket: {ticket.id}",
        f"Ticket Role: {ticket.ticket_role}",
        f"Dependency Group: {ticket.dependency_group}",
        f"Contracts Touched: {', '.join(ticket.contracts_touched) or 'none'}",
        f"Risk Class: {ticket.risk_class}",
        "",
        "Objective Impact:",
        ticket.objective_impact,
        "",
        "Owns:",
        *bullet_lines(ticket.owns),
        "",
        "Do Not Touch:",
        *bullet_lines(ticket.do_not_touch),
        "",
        "Validation:",
        *bullet_lines(ticket.validation),
        "",
        "Proof Packet Requirements:",
        *bullet_lines(ticket.proof_packet),
    ]
    return "\n".join(lines)


def bullet_lines(items: Sequence[str]) -> list[str]:
    if not items:
        return ["- none"]
    return [f"- `{item}`" for item in items]


def build_action(
    ticket: TicketRecord,
    pr: merge_steward.PullRequest,
    *,
    action: ReconciliationActionName,
    target_status: str | None,
    reason: str,
) -> ReconciliationAction:
    return ReconciliationAction(
        ticket_id=ticket.id,
        objective_id=ticket.objective_id,
        from_status=ticket.status,
        action=action,
        target_status=target_status,
        reason=reason,
        pr_number=pr.number,
        pr_url=pr.url,
        branch=pr.head_ref_name or None,
    )


def apply_reconciliation_action(
    connection: sqlite3.Connection,
    action: ReconciliationAction,
) -> None:
    if action.target_status is not None:
        work_ledger.transition_ticket(
            connection,
            ticket_id=action.ticket_id,
            status=action.target_status,
            actor=ACTOR,
            message=transition_message(action),
        )
    record_pr_evidence(connection, action)


def transition_message(action: ReconciliationAction) -> str:
    pr_ref = f"PR #{action.pr_number}" if action.pr_number is not None else "PR"
    if action.pr_url:
        pr_ref = f"{pr_ref} {action.pr_url}"
    return f"{pr_ref}: {action.reason}"


def record_pr_evidence(
    connection: sqlite3.Connection,
    action: ReconciliationAction,
) -> None:
    if action.pr_url is None and action.branch is None:
        return

    row = connection.execute(
        "SELECT objective_id, status, branch, pr_url FROM tickets WHERE id = ?",
        (action.ticket_id,),
    ).fetchone()
    if row is None:
        raise VcsReconcilerError(f"ticket not found: {action.ticket_id}")
    if row["branch"] == action.branch and row["pr_url"] == action.pr_url:
        return

    now = work_ledger.utc_now()
    with connection:
        connection.execute(
            """
            UPDATE tickets
            SET branch = ?, pr_url = ?, updated_at = ?
            WHERE id = ?
            """,
            (action.branch, action.pr_url, now, action.ticket_id),
        )
        work_ledger.insert_event(
            connection,
            ticket_id=action.ticket_id,
            objective_id=row["objective_id"],
            event_type="vcs_pr_observed",
            from_status=None,
            to_status=row["status"],
            message=transition_message(action),
            actor=ACTOR,
            created_at=now,
        )


def render_actions(
    actions: Sequence[ReconciliationAction],
    *,
    output_format: OutputFormat,
    applied: bool,
) -> str:
    payload = {
        "applied": applied,
        "actions": [action_payload(action) for action in actions],
    }
    if output_format == "json":
        return json.dumps(payload, indent=2, sort_keys=True)
    prefix = "APPLIED" if applied else "DRY RUN"
    if not actions:
        return f"{prefix}: no matching PRs to reconcile."
    lines = [f"{prefix}: VCS reconciliation result"]
    for action in actions:
        target = action.target_status or "no ledger change"
        pr = f"PR #{action.pr_number}" if action.pr_number is not None else "no PR"
        lines.append(
            f"- {action.ticket_id} | {action.action} | {action.from_status} -> "
            f"{target} | {pr} | {action.reason}"
        )
    return "\n".join(lines)


def action_payload(action: ReconciliationAction) -> Mapping[str, object]:
    return {
        "ticket_id": action.ticket_id,
        "objective_id": action.objective_id,
        "from_status": action.from_status,
        "action": action.action,
        "target_status": action.target_status,
        "reason": action.reason,
        "pr_number": action.pr_number,
        "pr_url": action.pr_url,
        "branch": action.branch,
    }


if __name__ == "__main__":
    raise SystemExit(main())
