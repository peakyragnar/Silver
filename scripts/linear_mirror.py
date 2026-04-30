#!/usr/bin/env python
"""Mirror local Silver work-ledger tickets to Linear for Symphony visibility."""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import urllib.error
import urllib.request
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import yaml


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import work_ledger  # noqa: E402


WORKFLOW_PATH = ROOT / "WORKFLOW.md"
DEFAULT_LIMIT = 100

LEDGER_TO_LINEAR_STATE = {
    "Backlog": "Backlog",
    "Ready": "Todo",
    "Claimed": "In Progress",
    "In Progress": "In Progress",
    "Rework": "Rework",
    "Merging": "Merging",
    "Done": "Done",
    "Safety Review": "Safety Review",
    "Blocked": "Backlog",
    "Canceled": "Canceled",
    "Duplicate": "Duplicate",
}

MirrorActionName = Literal[
    "create",
    "update_state",
    "update_description",
    "update_state_and_description",
    "noop",
    "skip",
]
OutputFormat = Literal["text", "json"]


class LinearMirrorError(RuntimeError):
    """Raised when the Linear mirror cannot safely continue."""


@dataclass(frozen=True, slots=True)
class LinearState:
    id: str
    name: str


@dataclass(frozen=True, slots=True)
class LinearIssue:
    id: str
    identifier: str
    title: str
    description: str
    state: str
    team_id: str


@dataclass(frozen=True, slots=True)
class LinearProject:
    id: str
    issues: tuple[LinearIssue, ...]


@dataclass(frozen=True, slots=True)
class LedgerMirrorTicket:
    id: str
    objective_id: str
    sequence: int
    title: str
    purpose: str
    objective_impact: str
    technical_summary: str
    status: str
    ticket_role: str
    dependency_group: str
    contracts_touched: tuple[str, ...]
    risk_class: str
    conflict_domain: str
    owns: tuple[str, ...]
    do_not_touch: tuple[str, ...]
    dependencies: tuple[str, ...]
    conflict_zones: tuple[str, ...]
    validation: tuple[str, ...]
    proof_packet: tuple[str, ...]
    branch: str | None
    pr_url: str | None
    latest_steward_event: str | None
    linear_identifier: str | None


@dataclass(frozen=True, slots=True)
class MirrorAction:
    action: MirrorActionName
    ticket: LedgerMirrorTicket
    target_state: str
    reason: str
    linear_issue: LinearIssue | None = None


class LinearClient:
    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
        self._team_state_cache: dict[str, Mapping[str, LinearState]] = {}

    def graphql(
        self,
        query: str,
        variables: Mapping[str, object] | None = None,
    ) -> Mapping[str, Any]:
        body = json.dumps({"query": query, "variables": variables or {}}).encode()
        request = urllib.request.Request(
            "https://api.linear.app/graphql",
            data=body,
            headers={
                "Authorization": self.api_key,
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                payload = json.load(response)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode(errors="replace")
            raise LinearMirrorError(
                f"Linear request failed with HTTP {exc.code}: {detail}"
            ) from exc
        except urllib.error.URLError as exc:
            raise LinearMirrorError(f"Linear request failed: {exc}") from exc

        if payload.get("errors"):
            raise LinearMirrorError(f"Linear returned errors: {payload['errors']}")
        return payload["data"]

    def project_snapshot(
        self,
        project_id_or_slug: str,
        *,
        limit: int,
    ) -> LinearProject:
        query = """
        query($project: String!, $limit: Int!) {
          project(id: $project) {
            id
            issues(first: $limit) {
              nodes {
                id
                identifier
                title
                description
                state { name }
                team { id }
              }
            }
          }
        }
        """
        data = self.graphql(query, {"project": project_id_or_slug, "limit": limit})
        project = data.get("project")
        if project is None:
            raise LinearMirrorError(f"Linear project not found: {project_id_or_slug!r}")
        return LinearProject(
            id=str(project["id"]),
            issues=tuple(
                LinearIssue(
                    id=node["id"],
                    identifier=node["identifier"],
                    title=node["title"],
                    description=str(node.get("description") or ""),
                    state=node["state"]["name"],
                    team_id=node["team"]["id"],
                )
                for node in project["issues"]["nodes"]
            ),
        )

    def team_states(self, team_id: str) -> Mapping[str, LinearState]:
        cached = self._team_state_cache.get(team_id)
        if cached is not None:
            return cached

        query = """
        query($team: String!) {
          team(id: $team) {
            states(first: 100) {
              nodes { id name }
            }
          }
        }
        """
        data = self.graphql(query, {"team": team_id})
        team = data.get("team")
        if team is None:
            raise LinearMirrorError(f"Linear team not found: {team_id!r}")
        states = {
            node["name"]: LinearState(id=node["id"], name=node["name"])
            for node in team["states"]["nodes"]
        }
        self._team_state_cache[team_id] = states
        return states

    def create_issue(
        self,
        *,
        team_id: str,
        project_id: str,
        state_id: str,
        title: str,
        description: str,
    ) -> LinearIssue:
        mutation = """
        mutation(
          $teamId: String!,
          $projectId: String!,
          $stateId: String!,
          $title: String!,
          $description: String!
        ) {
          issueCreate(input: {
            teamId: $teamId,
            projectId: $projectId,
            stateId: $stateId,
            title: $title,
            description: $description
          }) {
            issue {
              id
              identifier
              title
              description
              state { name }
              team { id }
            }
          }
        }
        """
        data = self.graphql(
            mutation,
            {
                "teamId": team_id,
                "projectId": project_id,
                "stateId": state_id,
                "title": title,
                "description": description,
            },
        )
        issue = data["issueCreate"]["issue"]
        return LinearIssue(
            id=issue["id"],
            identifier=issue["identifier"],
            title=issue["title"],
            description=str(issue.get("description") or ""),
            state=issue["state"]["name"],
            team_id=issue["team"]["id"],
        )

    def update_issue_state(self, issue_id: str, state_id: str) -> None:
        mutation = """
        mutation($issueId: String!, $stateId: String!) {
          issueUpdate(id: $issueId, input: { stateId: $stateId }) {
            success
          }
        }
        """
        self.graphql(mutation, {"issueId": issue_id, "stateId": state_id})

    def update_issue_description(self, issue_id: str, description: str) -> None:
        mutation = """
        mutation($issueId: String!, $description: String!) {
          issueUpdate(id: $issueId, input: { description: $description }) {
            success
          }
        }
        """
        self.graphql(
            mutation,
            {"issueId": issue_id, "description": description},
        )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, default=work_ledger.default_ledger_path())
    parser.add_argument(
        "--project",
        default=read_workflow_project_slug(WORKFLOW_PATH),
        help="Linear project ID or slug; defaults to tracker.project_slug",
    )
    parser.add_argument(
        "--team-id",
        default="",
        help="Linear team id to use when creating issues and the project has no issues",
    )
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="write create/update actions to Linear; default is dry-run",
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
    try:
        if args.check:
            print(check_configuration(args))
            return 0

        api_key = os.environ.get("LINEAR_API_KEY")
        if not api_key:
            raise LinearMirrorError("LINEAR_API_KEY is required")

        client = LinearClient(api_key)
        with work_ledger.connect_existing(args.ledger) as connection:
            actions = mirror_once(
                connection=connection,
                client=client,
                project=args.project,
                team_id=args.team_id or None,
                limit=args.limit,
                apply=args.apply,
            )
        print(render_actions(actions, args.format, apply=args.apply))
    except (LinearMirrorError, work_ledger.WorkLedgerError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


def mirror_once(
    *,
    connection: sqlite3.Connection,
    client: LinearClient,
    project: str,
    team_id: str | None,
    limit: int,
    apply: bool,
) -> tuple[MirrorAction, ...]:
    validate_limit(limit)
    if not project:
        raise LinearMirrorError("Linear project is not configured")

    tickets = mirror_tickets(connection)
    snapshot = client.project_snapshot(project, limit=limit)
    issue_by_ticket_id = issues_by_ledger_ticket_id(snapshot.issues)
    issue_by_identifier = {
        issue.identifier: issue
        for issue in snapshot.issues
    }
    create_team_id = team_id or first_team_id(snapshot.issues)
    actions = tuple(
        decide_mirror_action(
            ticket=ticket,
            issue=matching_issue(
                ticket,
                issue_by_ticket_id=issue_by_ticket_id,
                issue_by_identifier=issue_by_identifier,
            ),
        )
        for ticket in tickets
    )

    if not apply:
        return actions

    for action in actions:
        apply_mirror_action(
            connection=connection,
            client=client,
            action=action,
            project_id=snapshot.id,
            team_id=create_team_id,
        )
    return actions


def decide_mirror_action(
    *,
    ticket: LedgerMirrorTicket,
    issue: LinearIssue | None,
) -> MirrorAction:
    target_state = ledger_to_linear_state(ticket.status)
    if issue is None:
        return MirrorAction(
            action="create",
            ticket=ticket,
            target_state=target_state,
            reason="no Linear issue is linked to this ledger ticket",
        )
    state_mismatch = issue.state != target_state
    description_mismatch = not descriptions_match(
        issue.description,
        linear_description(ticket),
    )
    if state_mismatch and description_mismatch:
        return MirrorAction(
            action="update_state_and_description",
            ticket=ticket,
            target_state=target_state,
            reason=(
                f"Linear state is {issue.state}, expected {target_state}; "
                "description metadata is stale"
            ),
            linear_issue=issue,
        )
    if state_mismatch:
        return MirrorAction(
            action="update_state",
            ticket=ticket,
            target_state=target_state,
            reason=f"Linear state is {issue.state}, expected {target_state}",
            linear_issue=issue,
        )
    if description_mismatch:
        return MirrorAction(
            action="update_description",
            ticket=ticket,
            target_state=target_state,
            reason="Linear description metadata is stale",
            linear_issue=issue,
        )
    return MirrorAction(
        action="noop",
        ticket=ticket,
        target_state=target_state,
        reason="Linear mirror is current",
        linear_issue=issue,
    )


def apply_mirror_action(
    *,
    connection: sqlite3.Connection,
    client: LinearClient,
    action: MirrorAction,
    project_id: str,
    team_id: str | None,
) -> None:
    if action.action == "noop" or action.action == "skip":
        return

    if team_id is None:
        raise LinearMirrorError(
            "cannot create Linear issues without --team-id when the project has no issues"
        )

    states = client.team_states(team_id)
    target_state = states.get(action.target_state)
    if target_state is None:
        raise LinearMirrorError(
            f"Linear state {action.target_state!r} not found for team {team_id}"
        )

    now = work_ledger.utc_now()
    if action.action == "create":
        created = client.create_issue(
            team_id=team_id,
            project_id=project_id,
            state_id=target_state.id,
            title=linear_title(action.ticket),
            description=linear_description(action.ticket),
        )
        record_linear_sync(connection, action.ticket, created, action.target_state, now)
        return

    if action.action in ("update_state", "update_state_and_description"):
        if action.linear_issue is None:
            raise LinearMirrorError(f"{action.ticket.id}: missing Linear issue")
        client.update_issue_state(action.linear_issue.id, target_state.id)
        if action.action == "update_state_and_description":
            client.update_issue_description(
                action.linear_issue.id,
                linear_description(action.ticket),
            )
        record_linear_sync(
            connection,
            action.ticket,
            action.linear_issue,
            action.target_state,
            now,
        )
        return

    if action.action == "update_description":
        if action.linear_issue is None:
            raise LinearMirrorError(f"{action.ticket.id}: missing Linear issue")
        client.update_issue_description(
            action.linear_issue.id,
            linear_description(action.ticket),
        )
        record_linear_sync(
            connection,
            action.ticket,
            action.linear_issue,
            action.target_state,
            now,
        )
        return

    raise LinearMirrorError(f"unsupported mirror action: {action.action}")


def mirror_tickets(connection: sqlite3.Connection) -> tuple[LedgerMirrorTicket, ...]:
    rows = connection.execute(
        """
        SELECT
          tickets.*,
          (
            SELECT message
            FROM ticket_events
            WHERE ticket_events.ticket_id = tickets.id
              AND ticket_events.actor IN (
                'integration_steward',
                'vcs_reconciler',
                'merge_steward'
              )
            ORDER BY ticket_events.id DESC
            LIMIT 1
          ) AS latest_steward_event
        FROM tickets
        ORDER BY tickets.objective_id, tickets.sequence
        """
    ).fetchall()
    return tuple(row_to_mirror_ticket(row) for row in rows)


def row_to_mirror_ticket(row: sqlite3.Row) -> LedgerMirrorTicket:
    return LedgerMirrorTicket(
        id=row["id"],
        objective_id=row["objective_id"],
        sequence=int(row["sequence"]),
        title=row["title"],
        purpose=row["purpose"],
        objective_impact=row["objective_impact"],
        technical_summary=row["technical_summary"],
        status=row["status"],
        ticket_role=row["ticket_role"],
        dependency_group=row["dependency_group"],
        contracts_touched=tuple(work_ledger.loads_json(row["contracts_touched_json"])),
        risk_class=row["risk_class"],
        conflict_domain=row["conflict_domain"],
        owns=tuple(work_ledger.loads_json(row["owns_json"])),
        do_not_touch=tuple(work_ledger.loads_json(row["do_not_touch_json"])),
        dependencies=tuple(work_ledger.loads_json(row["dependencies_json"])),
        conflict_zones=tuple(work_ledger.loads_json(row["conflict_zones_json"])),
        validation=tuple(work_ledger.loads_json(row["validation_json"])),
        proof_packet=tuple(work_ledger.loads_json(row["proof_packet_json"])),
        branch=row["branch"],
        pr_url=row["pr_url"],
        latest_steward_event=row["latest_steward_event"],
        linear_identifier=row["linear_identifier"],
    )


def descriptions_match(current: str, expected: str) -> bool:
    return normalize_linear_description(current) == normalize_linear_description(expected)


def normalize_linear_description(description: str) -> str:
    """Normalize Linear's Markdown readback quirks before comparing text."""
    normalized_lines: list[str] = []
    lines = [line.rstrip() for line in description.strip().splitlines()]
    for index, line in enumerate(lines):
        stripped = line.strip()
        previous = normalized_lines[-1].strip() if normalized_lines else ""
        next_line = lines[index + 1].strip() if index + 1 < len(lines) else ""
        if (
            not stripped
            and previous.endswith(":")
            and next_line.startswith(("* ", "- "))
        ):
            continue
        line = line.replace(r"\`", "`")
        line = re.sub(r"`{3,}", "``", line)
        if stripped.startswith("* "):
            line = line.replace("* ", "- ", 1)
        normalized_lines.append(line)
    return "\n".join(normalized_lines).strip()


def matching_issue(
    ticket: LedgerMirrorTicket,
    *,
    issue_by_ticket_id: Mapping[str, LinearIssue],
    issue_by_identifier: Mapping[str, LinearIssue],
) -> LinearIssue | None:
    if ticket.linear_identifier:
        issue = issue_by_identifier.get(ticket.linear_identifier)
        if issue is not None:
            return issue
    return issue_by_ticket_id.get(ticket.id)


def issues_by_ledger_ticket_id(
    issues: Sequence[LinearIssue],
) -> Mapping[str, LinearIssue]:
    return {
        ticket_id: issue
        for issue in issues
        for ticket_id in [ledger_ticket_id_from_description(issue.description)]
        if ticket_id is not None
    }


def ledger_ticket_id_from_description(description: str) -> str | None:
    match = re.search(r"(?im)^Ledger Ticket:\s*`?([a-z0-9][a-z0-9_-]*)`?\s*$", description)
    if match is None:
        return None
    return match.group(1)


def ledger_to_linear_state(status: str) -> str:
    try:
        return LEDGER_TO_LINEAR_STATE[status]
    except KeyError as exc:
        raise LinearMirrorError(f"unsupported ledger status for Linear mirror: {status}") from exc


def first_team_id(issues: Sequence[LinearIssue]) -> str | None:
    if not issues:
        return None
    return issues[0].team_id


def record_linear_sync(
    connection: sqlite3.Connection,
    ticket: LedgerMirrorTicket,
    issue: LinearIssue,
    linear_state: str,
    synced_at: str,
) -> None:
    with connection:
        connection.execute(
            """
            UPDATE tickets
            SET linear_identifier = ?, updated_at = ?
            WHERE id = ?
            """,
            (issue.identifier, synced_at, ticket.id),
        )
        connection.execute(
            """
            INSERT INTO linear_mirror_state (
            ticket_id, linear_identifier, linear_state, last_synced_at, last_error
            )
            VALUES (?, ?, ?, ?, NULL)
            ON CONFLICT(ticket_id) DO UPDATE SET
              linear_identifier = excluded.linear_identifier,
              linear_state = excluded.linear_state,
              last_synced_at = excluded.last_synced_at,
              last_error = NULL
            """,
            (ticket.id, issue.identifier, linear_state, synced_at),
        )
        work_ledger.insert_event(
            connection,
            ticket_id=ticket.id,
            objective_id=ticket.objective_id,
            event_type="linear_mirror_synced",
            from_status=None,
            to_status=ticket.status,
            message=f"Mirrored to Linear issue {issue.identifier}.",
            actor="linear_mirror",
            created_at=synced_at,
        )


def linear_title(ticket: LedgerMirrorTicket) -> str:
    return f"{ticket.id}: {ticket.title}"


def linear_description(ticket: LedgerMirrorTicket) -> str:
    lines = [
        f"Parent Objective: {ticket.objective_id}",
        f"Ledger Ticket: {ticket.id}",
        f"Ticket Role: {ticket.ticket_role}",
        f"Dependency Group: {ticket.dependency_group}",
        f"Contracts Touched: {', '.join(ticket.contracts_touched) or 'none'}",
        f"Risk Class: {ticket.risk_class}",
        f"PR URL: {ticket.pr_url or 'none'}",
        f"Branch: {ticket.branch or 'none'}",
        "",
        "Purpose:",
        ticket.purpose,
        "",
        "Objective Impact:",
        ticket.objective_impact,
        "",
        "Technical Summary:",
        ticket.technical_summary,
        "",
        "Conflict Domain:",
        ticket.conflict_domain,
        "",
        "Owns:",
        *bullet_lines(ticket.owns),
        "",
        "Do Not Touch:",
        *bullet_lines(ticket.do_not_touch),
        "",
        "Dependencies:",
        *bullet_lines(ticket.dependencies),
        "",
        "Conflict Zones:",
        *bullet_lines(ticket.conflict_zones),
        "",
        "Validation:",
        *bullet_lines(ticket.validation),
        "",
        "Proof Packet Requirements:",
        *bullet_lines(ticket.proof_packet),
    ]
    if ticket.latest_steward_event:
        lines.extend(
            [
                "",
                "Latest Steward Event:",
                ticket.latest_steward_event,
            ]
        )
    return "\n".join(lines).strip() + "\n"


def bullet_lines(items: Sequence[str]) -> list[str]:
    if not items:
        return ["- none"]
    return [f"- `{item}`" for item in items]


def render_actions(
    actions: Sequence[MirrorAction],
    output_format: OutputFormat,
    *,
    apply: bool,
) -> str:
    payload = {
        "apply": apply,
        "actions": [action_payload(action) for action in actions],
    }
    if output_format == "json":
        return json.dumps(payload, indent=2, sort_keys=True)
    if not actions:
        return "No ledger tickets to mirror."
    prefix = "APPLY" if apply else "DRY RUN"
    lines = [f"{prefix}: Linear mirror actions"]
    for action in actions:
        issue = action.linear_issue.identifier if action.linear_issue else "new"
        lines.append(
            f"- {action.ticket.id} | {action.action} | {issue} -> "
            f"{action.target_state} | {action.reason}"
        )
    return "\n".join(lines)


def action_payload(action: MirrorAction) -> dict[str, object]:
    return {
        "action": action.action,
        "ticket_id": action.ticket.id,
        "title": action.ticket.title,
        "ledger_status": action.ticket.status,
        "target_linear_state": action.target_state,
        "reason": action.reason,
        "linear_identifier": (
            action.linear_issue.identifier if action.linear_issue else None
        ),
    }


def check_configuration(args: argparse.Namespace) -> str:
    lines = ["Linear mirror configuration check", ""]
    if not args.project:
        raise LinearMirrorError("Linear project is not configured")
    if not args.ledger:
        raise LinearMirrorError("ledger path is not configured")
    work_ledger.validate_positive_int(args.limit, "limit")
    lines.append(f"Ledger: {args.ledger}")
    lines.append(f"Linear project: {args.project}")
    lines.append(f"Limit: {args.limit}")
    lines.append("Result: local Linear mirror configuration is valid")
    return "\n".join(lines)


def validate_limit(limit: int) -> None:
    work_ledger.validate_positive_int(limit, "limit")


def read_workflow_project_slug(path: Path) -> str:
    if not path.exists():
        return ""
    text = path.read_text(encoding="utf-8")
    match = re.match(r"^---\n(.*?)\n---", text, flags=re.DOTALL)
    if not match:
        return ""
    data = yaml.safe_load(match.group(1)) or {}
    if not isinstance(data, Mapping):
        return ""
    tracker = data.get("tracker")
    if not isinstance(tracker, Mapping):
        return ""
    return str(tracker.get("project_slug") or "")


if __name__ == "__main__":
    raise SystemExit(main())
