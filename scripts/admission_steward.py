#!/usr/bin/env python
"""Promote approved Linear Backlog work into Todo up to safe capacity."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import yaml


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_PATH = ROOT / "WORKFLOW.md"
ACTIVE_OBJECTIVE_DIR = ROOT / "docs" / "objectives" / "active"

BACKLOG_STATE = "Backlog"
TODO_STATE = "Todo"
ACTIVE_STATES = frozenset(("Todo", "In Progress", "Rework"))
NON_ADMISSION_STATES = frozenset(
    ("Merging", "Safety Review", "Done", "Canceled", "Duplicate")
)
TERMINAL_STATES = frozenset(("Done", "Canceled", "Duplicate"))
DEFAULT_MAX_ACTIVE = 5
DEFAULT_TODO_BUFFER = 5
DEFAULT_WATCH_POLL_INTERVAL = 300
MIN_WATCH_POLL_INTERVAL = 120
DEFAULT_RATE_LIMIT_BACKOFF_SECONDS = 3600
HARD_EXACT_PATHS = frozenset(
    (
        "WORKFLOW.md",
        "scripts/admission_steward.py",
        "scripts/merge_steward.py",
        "scripts/planning_steward.py",
    )
)

AdmissionAction = Literal["promote", "wait", "skip"]


class AdmissionStewardError(RuntimeError):
    """Raised when the admission steward cannot safely continue."""


class LinearRateLimitError(AdmissionStewardError):
    """Raised when Linear asks the steward to stop polling for a while."""

    def __init__(self, message: str, retry_after_seconds: int) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


@dataclass(frozen=True, slots=True)
class LinearState:
    id: str
    name: str


@dataclass(frozen=True, slots=True)
class IssueRelation:
    type: str
    related_identifier: str
    related_state: str


@dataclass(frozen=True, slots=True)
class LinearIssue:
    id: str
    identifier: str
    title: str
    url: str
    description: str
    state: str
    team_states: Mapping[str, LinearState]
    relations: tuple[IssueRelation, ...] = ()


@dataclass(frozen=True, slots=True)
class PullRequest:
    title: str
    head_ref_name: str
    body: str
    state: str


@dataclass(frozen=True, slots=True)
class AdmissionDecision:
    action: AdmissionAction
    reason: str


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
            if exc.code == 429:
                retry_after = parse_retry_after_seconds(
                    exc.headers.get("Retry-After")
                )
                raise LinearRateLimitError(
                    f"Linear rate limit reached: {detail}",
                    retry_after,
                ) from exc
            raise AdmissionStewardError(
                f"Linear request failed with HTTP {exc.code}: {detail}"
            ) from exc
        except urllib.error.URLError as exc:
            raise AdmissionStewardError(f"Linear request failed: {exc}") from exc

        errors = payload.get("errors")
        if errors:
            retry_after = linear_errors_retry_after_seconds(errors)
            if retry_after is not None:
                raise LinearRateLimitError(
                    f"Linear rate limit reached: {errors}",
                    retry_after,
                )
            raise AdmissionStewardError(f"Linear returned errors: {errors}")
        return payload["data"]

    def project_issues(self, project_id_or_slug: str) -> tuple[LinearIssue, ...]:
        query = """
        query($project: String!) {
          project(id: $project) {
            issues(first: 100) {
              nodes {
                id
                identifier
                title
                url
                description
                state { name }
                team { id }
                relations {
                  nodes {
                    type
                    relatedIssue {
                      identifier
                      state { name }
                    }
                  }
                }
              }
            }
          }
        }
        """
        data = self.graphql(query, {"project": project_id_or_slug})
        project = data.get("project")
        if project is None:
            raise AdmissionStewardError(
                f"Linear project not found: {project_id_or_slug!r}"
            )

        issue_nodes = project["issues"]["nodes"]
        if not issue_nodes:
            return ()

        states_by_team = {
            team_id: self.team_states(team_id)
            for team_id in {node["team"]["id"] for node in issue_nodes}
        }
        issues: list[LinearIssue] = []
        for node in issue_nodes:
            issues.append(
                LinearIssue(
                    id=node["id"],
                    identifier=node["identifier"],
                    title=node["title"],
                    url=node["url"],
                    description=str(node.get("description") or ""),
                    state=node["state"]["name"],
                    team_states=states_by_team[node["team"]["id"]],
                    relations=parse_issue_relations(node),
                )
            )
        return tuple(issues)

    def issue_relations(self, issue_id: str) -> tuple[IssueRelation, ...]:
        query = """
        query($issue: String!) {
          issue(id: $issue) {
            relations {
              nodes {
                type
                relatedIssue {
                  identifier
                  state { name }
                }
              }
            }
          }
        }
        """
        data = self.graphql(query, {"issue": issue_id})
        issue = data.get("issue")
        if issue is None:
            raise AdmissionStewardError(f"Linear issue not found: {issue_id!r}")
        return tuple(
            IssueRelation(
                type=relation["type"],
                related_identifier=relation["relatedIssue"]["identifier"],
                related_state=relation["relatedIssue"]["state"]["name"],
            )
            for relation in issue["relations"]["nodes"]
            if relation.get("relatedIssue")
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
            raise AdmissionStewardError(f"Linear team not found: {team_id!r}")
        states = {
            node["name"]: LinearState(id=node["id"], name=node["name"])
            for node in team["states"]["nodes"]
        }
        self._team_state_cache[team_id] = states
        return states

    def create_comment(self, issue_id: str, body: str) -> None:
        mutation = """
        mutation($issueId: String!, $body: String!) {
          commentCreate(input: { issueId: $issueId, body: $body }) {
            success
          }
        }
        """
        self.graphql(mutation, {"issueId": issue_id, "body": body})

    def update_issue_state(self, issue_id: str, state_id: str) -> None:
        mutation = """
        mutation($issueId: String!, $stateId: String!) {
          issueUpdate(id: $issueId, input: { stateId: $stateId }) {
            success
          }
        }
        """
        self.graphql(mutation, {"issueId": issue_id, "stateId": state_id})


class GitHubClient:
    def __init__(
        self,
        repo: str,
        *,
        runner: "CommandRunner | None" = None,
    ) -> None:
        self.repo = repo
        self.runner = runner or CommandRunner()

    def list_open_pull_requests(self, limit: int) -> tuple[PullRequest, ...]:
        payload = self.runner.json(
            [
                "gh",
                "pr",
                "list",
                "--repo",
                self.repo,
                "--state",
                "open",
                "--limit",
                str(limit),
                "--json",
                "title,headRefName,body,state",
            ]
        )
        return tuple(
            PullRequest(
                title=item["title"],
                head_ref_name=item["headRefName"],
                body=str(item.get("body") or ""),
                state=item["state"],
            )
            for item in payload
        )


class CommandRunner:
    def run(self, command: Sequence[str]) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            list(command),
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip()
            raise AdmissionStewardError(
                f"command failed ({' '.join(command)}): {detail}"
            )
        return result

    def json(self, command: Sequence[str]) -> Any:
        result = self.run(command)
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise AdmissionStewardError(
                f"command returned invalid JSON ({' '.join(command)})"
            ) from exc


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project",
        default=read_workflow_project_slug(WORKFLOW_PATH),
        help="Linear project ID or slug; defaults to tracker.project_slug",
    )
    parser.add_argument(
        "--repo",
        default=detect_github_repo(),
        help="GitHub repository as owner/name; defaults to origin",
    )
    parser.add_argument(
        "--max-active",
        type=int,
        default=DEFAULT_MAX_ACTIVE,
        help="maximum active Symphony tickets to allow",
    )
    parser.add_argument(
        "--todo-buffer",
        type=int,
        default=DEFAULT_TODO_BUFFER,
        help="maximum Todo tickets to keep queued",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="number of open GitHub PRs to inspect",
    )
    parser.add_argument(
        "--promote",
        action="store_true",
        help="write selected Backlog issues to Todo",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="read state and print decisions without writes; this is the default",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="keep polling and admitting work",
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=DEFAULT_WATCH_POLL_INTERVAL,
        help="seconds between --watch polls",
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
        validate_capacity_args(args.max_active, args.todo_buffer)
        if args.check:
            print(check_configuration(args=args))
            return 0

        api_key = os.environ.get("LINEAR_API_KEY")
        if not api_key:
            raise AdmissionStewardError("LINEAR_API_KEY is required")

        linear = LinearClient(api_key)
        github = GitHubClient(args.repo)
        if args.watch:
            watch(args=args, linear=linear, github=github)
            return 0

        run_once(args=args, linear=linear, github=github)
    except AdmissionStewardError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


def watch(
    *,
    args: argparse.Namespace,
    linear: LinearClient,
    github: GitHubClient,
) -> None:
    poll_interval = effective_watch_poll_interval(args.poll_interval)
    if poll_interval != args.poll_interval:
        print(
            (
                "Admission steward poll interval raised from "
                f"{args.poll_interval}s to {poll_interval}s to protect "
                "the Linear API limit."
            ),
            file=sys.stderr,
        )

    while True:
        try:
            run_once(args=args, linear=linear, github=github)
        except LinearRateLimitError as exc:
            sleep_seconds = max(exc.retry_after_seconds, poll_interval)
            print(
                f"Linear rate limited; sleeping {sleep_seconds}s before retry.",
                file=sys.stderr,
            )
            time.sleep(sleep_seconds)
            continue
        time.sleep(poll_interval)


def run_once(
    *,
    args: argparse.Namespace,
    linear: LinearClient,
    github: GitHubClient,
) -> None:
    issues = linear.project_issues(args.project)
    open_pull_requests = github.list_open_pull_requests(args.limit)
    approved_objectives = active_objective_ids(ROOT)
    decisions = select_admissions(
        issues=issues,
        open_pull_requests=open_pull_requests,
        approved_objectives=approved_objectives,
        max_active=args.max_active,
        todo_buffer=args.todo_buffer,
    )

    writable = bool(args.promote and not args.dry_run)
    for issue, decision in decisions:
        print(format_decision(issue, decision))
        if writable and decision.action == "promote":
            apply_promotion(issue=issue, decision=decision, linear=linear)

    if not decisions:
        print("No Backlog issues to admit.")
    elif not writable:
        print("DRY RUN: no Linear issues were modified.")


def select_admissions(
    *,
    issues: Sequence[LinearIssue],
    open_pull_requests: Sequence[PullRequest],
    approved_objectives: set[str],
    max_active: int,
    todo_buffer: int,
) -> tuple[tuple[LinearIssue, AdmissionDecision], ...]:
    validate_capacity_args(max_active, todo_buffer)
    backlog = sorted(
        (issue for issue in issues if issue.state == BACKLOG_STATE),
        key=_issue_sort_key,
    )
    active = [issue for issue in issues if issue.state in ACTIVE_STATES]
    active_count = len(active)
    todo_count = sum(1 for issue in issues if issue.state == TODO_STATE)
    slots = min(max_active - active_count, todo_buffer - todo_count)

    decisions: list[tuple[LinearIssue, AdmissionDecision]] = []
    selected: list[LinearIssue] = []
    for issue in backlog:
        if slots <= 0:
            decisions.append(
                (
                    issue,
                    AdmissionDecision(
                        "wait",
                        (
                            "capacity full: "
                            f"{active_count} active, {todo_count} Todo, "
                            f"max_active={max_active}, todo_buffer={todo_buffer}"
                        ),
                    ),
                )
            )
            continue

        decision = decide_issue_admission(
            issue=issue,
            issues=issues,
            active_issues=tuple(active + selected),
            open_pull_requests=open_pull_requests,
            approved_objectives=approved_objectives,
        )
        decisions.append((issue, decision))
        if decision.action == "promote":
            selected.append(issue)
            slots -= 1
            active_count += 1
            todo_count += 1
    return tuple(decisions)


def decide_issue_admission(
    *,
    issue: LinearIssue,
    issues: Sequence[LinearIssue],
    active_issues: Sequence[LinearIssue],
    open_pull_requests: Sequence[PullRequest],
    approved_objectives: set[str],
) -> AdmissionDecision:
    if issue.state != BACKLOG_STATE:
        return AdmissionDecision("skip", f"state is {issue.state}, not Backlog")

    objective_id = parent_objective(issue.description)
    if objective_id is None:
        return AdmissionDecision("skip", "missing Parent Objective")
    if objective_id not in approved_objectives:
        return AdmissionDecision(
            "skip",
            f"Parent Objective is not active/approved: {objective_id}",
        )

    blockers = unfinished_blockers(issue, issues)
    if blockers:
        return AdmissionDecision(
            "wait",
            "blocked by unfinished issue(s): " + ", ".join(blockers),
        )

    matching_pr = choose_open_pr_for_issue(issue, open_pull_requests)
    if matching_pr is not None:
        return AdmissionDecision(
            "wait",
            f"open PR already exists on branch {matching_pr.head_ref_name}",
        )

    start_safety = start_safety_trigger(issue.description)
    if start_safety is not None:
        return AdmissionDecision("wait", start_safety)

    conflict = hard_conflict(issue, active_issues)
    if conflict is not None:
        return AdmissionDecision("wait", conflict)

    return AdmissionDecision(
        "promote",
        f"approved Objective {objective_id}; capacity available",
    )


def apply_promotion(
    *,
    issue: LinearIssue,
    decision: AdmissionDecision,
    linear: LinearClient,
) -> None:
    todo_state = issue.team_states.get(TODO_STATE)
    if todo_state is None:
        raise AdmissionStewardError(f"{issue.identifier}: Linear state Todo not found")
    linear.create_comment(issue.id, build_admission_comment(issue, decision))
    linear.update_issue_state(issue.id, todo_state.id)


def build_admission_comment(issue: LinearIssue, decision: AdmissionDecision) -> str:
    objective = parent_objective(issue.description) or "unknown"
    return (
        "## Admission Steward\n\n"
        f"Decision: `{decision.action}`\n\n"
        f"Objective: `{objective}`\n\n"
        f"Reason: {decision.reason}\n\n"
        "Moved to `Todo` so Symphony can build it under the approved Objective. "
        "Routine conflicts should go through `Rework`; destructive or semantic "
        "exceptions should route to `Safety Review`."
    )


def active_objective_ids(root: Path = ROOT) -> set[str]:
    objective_dir = root / "docs" / "objectives" / "active"
    if not objective_dir.exists():
        return set()
    return {path.stem for path in objective_dir.glob("*.md")}


def parent_objective(description: str) -> str | None:
    match = re.search(
        r"(?im)^parent objective:\s*`?([a-z0-9][a-z0-9_-]*)`?\s*$",
        description,
    )
    if not match:
        return None
    return match.group(1)


def unfinished_blockers(
    issue: LinearIssue,
    issues: Sequence[LinearIssue],
) -> tuple[str, ...]:
    states_by_identifier = {item.identifier: item.state for item in issues}
    blockers: list[str] = []
    for item in issues:
        for relation in item.relations:
            if (
                relation.type == "blocks"
                and relation.related_identifier == issue.identifier
                and states_by_identifier.get(item.identifier) not in TERMINAL_STATES
            ):
                blockers.append(item.identifier)
            if (
                item.identifier == issue.identifier
                and relation.type in {"blocked_by", "blocks"}
                and relation.related_state not in TERMINAL_STATES
                and relation.related_identifier != issue.identifier
            ):
                # Some Linear relation views expose the inverse directly.
                if relation.type == "blocked_by":
                    blockers.append(relation.related_identifier)
    return tuple(sorted(set(blockers), key=_identifier_sort_key))


def parse_issue_relations(node: Mapping[str, Any]) -> tuple[IssueRelation, ...]:
    relations = node.get("relations")
    if not isinstance(relations, Mapping):
        return ()
    nodes = relations.get("nodes")
    if not isinstance(nodes, Sequence):
        return ()
    parsed: list[IssueRelation] = []
    for relation in nodes:
        if not isinstance(relation, Mapping):
            continue
        related_issue = relation.get("relatedIssue")
        if not isinstance(related_issue, Mapping):
            continue
        state = related_issue.get("state")
        if not isinstance(state, Mapping):
            continue
        parsed.append(
            IssueRelation(
                type=str(relation.get("type") or ""),
                related_identifier=str(related_issue.get("identifier") or ""),
                related_state=str(state.get("name") or ""),
            )
        )
    return tuple(
        relation
        for relation in parsed
        if relation.type and relation.related_identifier and relation.related_state
    )


def choose_open_pr_for_issue(
    issue: LinearIssue,
    pull_requests: Sequence[PullRequest],
) -> PullRequest | None:
    identifier = issue.identifier.lower()
    for pr in pull_requests:
        haystack = " ".join((pr.title, pr.head_ref_name, pr.body)).lower()
        if identifier in haystack:
            return pr
    return None


def start_safety_trigger(description: str) -> str | None:
    if re.search(r"(?im)^safety review required before start:\s*(yes|true)\s*$", description):
        return "Safety Review required before start"
    return None


def hard_conflict(
    issue: LinearIssue,
    active_issues: Sequence[LinearIssue],
) -> str | None:
    issue_paths = issue_owned_paths(issue.description)
    issue_has_migration = has_migration_ownership(issue, issue_paths)
    for active in active_issues:
        active_paths = issue_owned_paths(active.description)
        if issue_has_migration and has_migration_ownership(active, active_paths):
            return f"migration/schema lane occupied by {active.identifier}"

        shared_hard_paths = sorted(
            path for path in issue_paths & active_paths if path in HARD_EXACT_PATHS
        )
        if shared_hard_paths:
            return (
                f"hard conflict with {active.identifier} on "
                + ", ".join(shared_hard_paths)
            )
    return None


def has_migration_ownership(issue: LinearIssue, paths: set[str]) -> bool:
    if any(path == "db/migrations" or path.startswith("db/migrations/") for path in paths):
        return True
    return bool(re.search(r"\b(schema|migration|migrations)\b", issue.title, re.I))


def issue_owned_paths(description: str) -> set[str]:
    paths: set[str] = set()
    for heading in ("Owns", "Conflict Zones"):
        for item in section_items(description, heading):
            path = normalize_path(item)
            if path:
                paths.add(path)
    return paths


def section_items(description: str, heading: str) -> tuple[str, ...]:
    pattern = re.compile(
        rf"(?ims)^{re.escape(heading)}:\s*\n(?P<body>.*?)(?:\n[A-Z][A-Za-z ]+:\n|\Z)"
    )
    match = pattern.search(description)
    if not match:
        return ()
    items: list[str] = []
    for line in match.group("body").splitlines():
        stripped = line.strip()
        if not stripped or stripped.lower() == "- none":
            continue
        stripped = re.sub(r"^[-*]\s*", "", stripped)
        items.append(stripped)
    return tuple(items)


def normalize_path(value: str) -> str | None:
    candidate = value.strip().strip("`").strip()
    if not candidate or candidate.lower() == "none":
        return None
    candidate = candidate.split(" only if ", maxsplit=1)[0]
    candidate = candidate.split(" unless ", maxsplit=1)[0]
    candidate = candidate.split(" ", maxsplit=1)[0]
    candidate = candidate.strip().strip("`").strip().rstrip("/")
    if not candidate or candidate == ".":
        return None
    return candidate


def format_decision(issue: LinearIssue, decision: AdmissionDecision) -> str:
    return f"{issue.identifier} | {decision.action} | {issue.state} | {decision.reason}"


def check_configuration(args: argparse.Namespace) -> str:
    lines = ["Admission steward configuration check", ""]
    if not args.project:
        raise AdmissionStewardError("Linear project is not configured")
    if not args.repo:
        raise AdmissionStewardError("GitHub repo is not configured")
    if shutil.which("gh") is None:
        raise AdmissionStewardError("GitHub CLI `gh` is required")
    if not ACTIVE_OBJECTIVE_DIR.exists():
        raise AdmissionStewardError("active Objective directory is missing")

    lines.append(f"Linear project: {args.project}")
    lines.append(f"GitHub repo: {args.repo}")
    lines.append(f"Max active: {args.max_active}")
    lines.append(f"Todo buffer: {args.todo_buffer}")
    lines.append("Active Objectives: " + ", ".join(sorted(active_objective_ids(ROOT))))
    lines.append("Result: local admission steward configuration is valid")
    return "\n".join(lines)


def validate_capacity_args(max_active: int, todo_buffer: int) -> None:
    if isinstance(max_active, bool) or max_active < 1:
        raise AdmissionStewardError("max_active must be a positive integer")
    if isinstance(todo_buffer, bool) or todo_buffer < 1:
        raise AdmissionStewardError("todo_buffer must be a positive integer")


def effective_watch_poll_interval(poll_interval: int) -> int:
    if isinstance(poll_interval, bool) or poll_interval < 1:
        raise AdmissionStewardError("poll_interval must be a positive integer")
    return max(poll_interval, MIN_WATCH_POLL_INTERVAL)


def parse_retry_after_seconds(value: str | None) -> int:
    if value is None:
        return DEFAULT_RATE_LIMIT_BACKOFF_SECONDS
    try:
        parsed = int(value)
    except ValueError:
        return DEFAULT_RATE_LIMIT_BACKOFF_SECONDS
    return max(parsed, MIN_WATCH_POLL_INTERVAL)


def linear_errors_retry_after_seconds(errors: object) -> int | None:
    if not isinstance(errors, Sequence) or isinstance(errors, str):
        return None

    for error in errors:
        if not isinstance(error, Mapping):
            continue
        extensions = error.get("extensions")
        if not isinstance(extensions, Mapping):
            extensions = {}

        code = str(extensions.get("code") or "").upper()
        message = str(error.get("message") or "")
        if code != "RATELIMITED" and "rate limit" not in message.lower():
            continue

        duration = extensions.get("duration")
        if isinstance(duration, (int, float)) and duration > 0:
            return max(int((duration + 999) // 1000), MIN_WATCH_POLL_INTERVAL)
        return DEFAULT_RATE_LIMIT_BACKOFF_SECONDS

    return None


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


def detect_github_repo() -> str:
    try:
        result = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
    except OSError:
        return ""
    if result.returncode != 0:
        return ""
    return parse_github_repo(result.stdout.strip())


def parse_github_repo(remote: str) -> str:
    https_match = re.search(r"github\.com[:/]([^/]+/[^/.]+)(?:\.git)?$", remote)
    if not https_match:
        return ""
    return https_match.group(1)


def _issue_sort_key(issue: LinearIssue) -> tuple[str, int, str]:
    return _identifier_sort_key(issue.identifier)


def _identifier_sort_key(identifier: str) -> tuple[str, int, str]:
    match = re.match(r"([A-Z]+)-(\d+)$", identifier)
    if not match:
        return (identifier, 0, identifier)
    return (match.group(1), int(match.group(2)), identifier)


if __name__ == "__main__":
    raise SystemExit(main())
