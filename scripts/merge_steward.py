#!/usr/bin/env python
"""Queue approved Linear issues for merge without starting Codex workers."""

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
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Literal

import yaml


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_PATH = ROOT / "WORKFLOW.md"
DEFAULT_REQUIRED_CHECKS = ("Python 3.10 checks",)
DEFAULT_WATCH_POLL_INTERVAL = 300
MIN_WATCH_POLL_INTERVAL = 120
DEFAULT_RATE_LIMIT_BACKOFF_SECONDS = 3600
MERGING_STATE = "Merging"
SAFETY_REVIEW_STATE = "Safety Review"
TERMINAL_STATE_NAMES = frozenset(("Done", "Canceled", "Duplicate"))

IssueAction = Literal[
    "queue",
    "mark_done",
    "stale_mark_done",
    "move_rework",
    "move_safety_review",
    "wait",
    "skip",
]


class MergeStewardError(RuntimeError):
    """Raised when the merge steward cannot safely continue."""


class LinearRateLimitError(MergeStewardError):
    """Raised when Linear asks the steward to stop polling for a while."""

    def __init__(self, message: str, retry_after_seconds: int) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


@dataclass(frozen=True, slots=True)
class LinearState:
    id: str
    name: str


@dataclass(frozen=True, slots=True)
class LinearIssue:
    id: str
    identifier: str
    title: str
    url: str
    description: str
    state: str
    team_states: Mapping[str, LinearState]


@dataclass(frozen=True, slots=True)
class CheckRun:
    name: str
    status: str | None
    conclusion: str | None


@dataclass(frozen=True, slots=True)
class ChangedFile:
    path: str
    additions: int | None
    deletions: int | None


@dataclass(frozen=True, slots=True)
class PullRequest:
    number: int
    title: str
    url: str
    state: str
    head_ref_name: str
    body: str
    merged_at: str | None
    is_draft: bool
    merge_state_status: str | None
    auto_merge_enabled: bool
    in_merge_queue: bool
    changed_files: tuple[ChangedFile, ...]
    diff: str | None
    checks: tuple[CheckRun, ...]


@dataclass(frozen=True, slots=True)
class Decision:
    action: IssueAction
    reason: str
    pr_number: int | None = None


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
            raise MergeStewardError(
                f"Linear request failed with HTTP {exc.code}: {detail}"
            ) from exc
        except urllib.error.URLError as exc:
            raise MergeStewardError(f"Linear request failed: {exc}") from exc

        errors = payload.get("errors")
        if errors:
            retry_after = linear_errors_retry_after_seconds(errors)
            if retry_after is not None:
                raise LinearRateLimitError(
                    f"Linear rate limit reached: {errors}",
                    retry_after,
                )
            raise MergeStewardError(f"Linear returned errors: {errors}")
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
              }
            }
          }
        }
        """
        data = self.graphql(query, {"project": project_id_or_slug})
        project = data.get("project")
        if project is None:
            raise MergeStewardError(
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
            state = node["state"]["name"]
            issues.append(
                LinearIssue(
                    id=node["id"],
                    identifier=node["identifier"],
                    title=node["title"],
                    url=node["url"],
                    description=str(node.get("description") or ""),
                    state=state,
                    team_states=states_by_team[node["team"]["id"]],
                )
            )
        return tuple(issues)

    def merging_issues(self, project_id_or_slug: str) -> tuple[LinearIssue, ...]:
        return merging_issue_candidates(self.project_issues(project_id_or_slug))

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
            raise MergeStewardError(f"Linear team not found: {team_id!r}")
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
        self.owner, self.name = repo.split("/", maxsplit=1)
        self.runner = runner or CommandRunner()

    def list_pull_requests(self, limit: int) -> tuple[PullRequest, ...]:
        command = [
            "gh",
            "pr",
            "list",
            "--repo",
            self.repo,
            "--state",
            "all",
            "--limit",
            str(limit),
            "--json",
            (
                "number,title,url,state,mergedAt,headRefName,body,isDraft,"
                "mergeStateStatus,autoMergeRequest,statusCheckRollup,files"
            ),
        ]
        payload = self.runner.json(command)
        queue_map = self._merge_queue_map(
            [int(item["number"]) for item in payload if item["state"] == "OPEN"]
        )
        return tuple(_parse_pull_request(item, queue_map) for item in payload)

    def with_diff(self, pr: PullRequest) -> PullRequest:
        try:
            result = self.runner.run(
                [
                    "gh",
                    "pr",
                    "diff",
                    str(pr.number),
                    "--repo",
                    self.repo,
                    "--patch",
                ]
            )
        except MergeStewardError:
            return pr
        return replace(pr, diff=result.stdout)

    def queue_pull_request(self, number: int) -> None:
        self.runner.run(
            [
                "gh",
                "pr",
                "merge",
                str(number),
                "--repo",
                self.repo,
                "--auto",
                "--merge",
            ]
        )

    def _merge_queue_map(self, numbers: Sequence[int]) -> Mapping[int, bool]:
        if not numbers:
            return {}

        fragments = "\n".join(
            (
                f"pr{number}: pullRequest(number: {number}) "
                "{ isInMergeQueue }"
            )
            for number in numbers
        )
        query = (
            "query($owner: String!, $name: String!) { "
            "repository(owner: $owner, name: $name) { "
            f"{fragments} "
            "} }"
        )
        payload = self.runner.json(
            [
                "gh",
                "api",
                "graphql",
                "-f",
                f"query={query}",
                "-F",
                f"owner={self.owner}",
                "-F",
                f"name={self.name}",
            ]
        )
        repository = payload["data"]["repository"]
        return {
            number: bool(repository[f"pr{number}"]["isInMergeQueue"])
            for number in numbers
        }


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
            raise MergeStewardError(
                f"command failed ({' '.join(command)}): {detail}"
            )
        return result

    def json(self, command: Sequence[str]) -> Any:
        result = self.run(command)
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise MergeStewardError(
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
        "--required-check",
        action="append",
        dest="required_checks",
        help=(
            "required PR check name; may be repeated. Defaults to "
            "'Python 3.10 checks'."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="number of recent GitHub PRs to inspect",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="read Linear/GitHub state and print actions without writes",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="keep polling for Merging issues",
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
    required_checks = tuple(args.required_checks or DEFAULT_REQUIRED_CHECKS)

    try:
        if args.check:
            print(check_configuration(args=args, required_checks=required_checks))
            return 0

        api_key = os.environ.get("LINEAR_API_KEY")
        if not api_key:
            raise MergeStewardError("LINEAR_API_KEY is required")

        linear = LinearClient(api_key)
        github = GitHubClient(args.repo)
        if args.watch:
            watch(args=args, linear=linear, github=github, required_checks=required_checks)
            return 0

        run_once(
            args=args,
            linear=linear,
            github=github,
            required_checks=required_checks,
        )
    except MergeStewardError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    return 0


def watch(
    *,
    args: argparse.Namespace,
    linear: LinearClient,
    github: GitHubClient,
    required_checks: Sequence[str],
) -> None:
    poll_interval = effective_watch_poll_interval(args.poll_interval)
    if poll_interval != args.poll_interval:
        print(
            (
                "Merge steward poll interval raised from "
                f"{args.poll_interval}s to {poll_interval}s to protect "
                "the Linear API limit."
            ),
            file=sys.stderr,
        )

    while True:
        try:
            run_once(
                args=args,
                linear=linear,
                github=github,
                required_checks=required_checks,
            )
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
    required_checks: Sequence[str],
) -> None:
    project_issues = linear.project_issues(args.project)
    merging_issues = merging_issue_candidates(project_issues)
    stale_issues = stale_reconciliation_candidates(project_issues)

    if not merging_issues:
        print("No Linear issues in Merging.")

    if not merging_issues and not stale_issues:
        print("No stale nonterminal Linear issues to reconcile.")
        return

    pull_requests = github.list_pull_requests(args.limit)
    for issue in merging_issues:
        pr = choose_pr_for_issue(issue, pull_requests)
        if pr is not None and pr.state == "OPEN":
            pr = github.with_diff(pr)
        decision = decide_issue_action(issue, pr, required_checks)
        print(format_decision(issue, decision, pr))
        if args.dry_run:
            continue
        apply_decision(issue=issue, pr=pr, decision=decision, linear=linear, github=github)

    stale_actions: list[tuple[LinearIssue, PullRequest | None, Decision]] = []
    for issue in stale_issues:
        pr = choose_pr_for_issue(issue, pull_requests)
        decision = decide_stale_issue_action(issue, pr)
        if decision.action != "stale_mark_done":
            continue
        stale_actions.append((issue, pr, decision))

    if not stale_actions:
        print("No stale merged Linear issues to reconcile.")
        return

    for issue, pr, decision in stale_actions:
        print(format_decision(issue, decision, pr))
        if args.dry_run:
            continue
        apply_decision(issue=issue, pr=pr, decision=decision, linear=linear, github=github)


def apply_decision(
    *,
    issue: LinearIssue,
    pr: PullRequest | None,
    decision: Decision,
    linear: LinearClient,
    github: GitHubClient,
) -> None:
    if decision.action == "queue":
        if decision.pr_number is None:
            raise MergeStewardError(f"{issue.identifier}: no PR number to queue")
        github.queue_pull_request(decision.pr_number)
        return

    if decision.action in {"mark_done", "stale_mark_done"}:
        done_state = issue.team_states.get("Done")
        if done_state is None:
            raise MergeStewardError(f"{issue.identifier}: Linear state Done not found")
        if pr is None:
            raise MergeStewardError(f"{issue.identifier}: no PR found to mark done")
        linear.create_comment(
            issue.id,
            (
                "## Merge Confirmation\n\n"
                f"PR merged into `main`: {pr.url}\n\n"
                "The merge steward verified GitHub reports the PR as merged."
            ),
        )
        linear.update_issue_state(issue.id, done_state.id)
        return

    if decision.action == "move_rework":
        rework_state = issue.team_states.get("Rework")
        if rework_state is None:
            raise MergeStewardError(f"{issue.identifier}: Linear state Rework not found")
        linear.create_comment(
            issue.id,
            (
                "## Merge Steward Blocker\n\n"
                f"{decision.reason}\n\n"
                "Moved to `Rework` so a Codex worker can repair the PR."
            ),
        )
        linear.update_issue_state(issue.id, rework_state.id)
        return

    if decision.action == "move_safety_review":
        safety_review_state = issue.team_states.get(SAFETY_REVIEW_STATE)
        if safety_review_state is None:
            raise MergeStewardError(
                f"{issue.identifier}: Linear state {SAFETY_REVIEW_STATE} not found"
            )
        if pr is None:
            raise MergeStewardError(
                f"{issue.identifier}: no PR found for Safety Review"
            )
        linear.create_comment(
            issue.id,
            build_safety_review_comment(issue=issue, pr=pr, trigger=decision.reason),
        )
        linear.update_issue_state(issue.id, safety_review_state.id)


def decide_issue_action(
    issue: LinearIssue,
    pr: PullRequest | None,
    required_checks: Sequence[str],
) -> Decision:
    if pr is None:
        return Decision(
            "skip",
            f"{issue.identifier}: no matching pull request found",
        )

    if pr.state == "MERGED" or pr.merged_at:
        return Decision("mark_done", "PR is merged", pr.number)

    if pr.state != "OPEN":
        return Decision("skip", f"PR is {pr.state.lower()}, not open", pr.number)

    if pr.is_draft:
        return Decision("wait", "PR is still draft", pr.number)

    safety_allowance: str | None = None
    safety_trigger = safety_review_trigger(issue, pr)
    if safety_trigger is not None:
        safety_allowance = planned_objective_safety_allowance(
            issue,
            pr,
            safety_trigger,
        )
        if safety_allowance is None:
            return Decision("move_safety_review", safety_trigger, pr.number)

    merge_state = (pr.merge_state_status or "UNKNOWN").upper()
    if merge_state == "DIRTY":
        return Decision(
            "move_rework",
            decision_reason(safety_allowance, "PR has merge conflicts"),
            pr.number,
        )

    check_status = required_check_status(pr, required_checks)
    if check_status.action == "move_rework":
        return Decision(
            "move_rework",
            decision_reason(safety_allowance, check_status.reason),
            pr.number,
        )
    if check_status.action == "wait":
        return Decision(
            "wait",
            decision_reason(safety_allowance, check_status.reason),
            pr.number,
        )

    if merge_state in {"BLOCKED", "UNKNOWN"}:
        return Decision(
            "wait",
            decision_reason(safety_allowance, f"PR merge state is {merge_state}"),
            pr.number,
        )
    if pr.in_merge_queue:
        return Decision(
            "wait",
            decision_reason(
                safety_allowance,
                "PR is already in GitHub merge queue",
            ),
            pr.number,
        )
    if pr.auto_merge_enabled:
        return Decision(
            "wait",
            decision_reason(safety_allowance, "PR auto-merge is already enabled"),
            pr.number,
        )

    return Decision(
        "queue",
        decision_reason(safety_allowance, "PR is approved, green, and mergeable"),
        pr.number,
    )


def decide_stale_issue_action(
    issue: LinearIssue,
    pr: PullRequest | None,
) -> Decision:
    if issue.state == MERGING_STATE:
        return Decision(
            "skip",
            f"{issue.identifier}: issue is already handled by Merging queue",
        )
    if issue.state in TERMINAL_STATE_NAMES:
        return Decision(
            "skip",
            f"{issue.identifier}: terminal Linear state {issue.state}",
        )
    if pr is None:
        return Decision(
            "skip",
            f"{issue.identifier}: no matching pull request found",
        )

    if pr.state == "MERGED" or pr.merged_at:
        return Decision(
            "stale_mark_done",
            f"stale nonterminal issue in {issue.state} has merged PR",
            pr.number,
        )

    return Decision(
        "skip",
        f"{issue.identifier}: matching PR is not merged ({pr.state.lower()})",
        pr.number,
    )


def safety_review_trigger(issue: LinearIssue, pr: PullRequest) -> str | None:
    paths = tuple(
        normalized
        for changed_file in pr.changed_files
        if (normalized := _normalize_path(changed_file.path))
    )
    added_diff = _added_diff_text(pr.diff or "")
    added_diff_by_path = _added_diff_text_by_path(pr.diff or "")
    pr_metadata = "\n".join((pr.title, pr.head_ref_name, pr.body))

    do_not_touch = _do_not_touch_trigger(issue.description, paths)
    if do_not_touch is not None:
        return do_not_touch

    destructive_path = _first_matching_path(paths, _is_db_or_data_path)
    destructive_diff = _diff_for_path(
        destructive_path,
        paths=paths,
        added_diff=added_diff,
        added_diff_by_path=added_diff_by_path,
    )
    if destructive_path is not None and _matches_any(
        destructive_diff,
        (
            r"\bdrop\s+(table|column|database|schema)\b",
            r"\btruncate\s+(table\s+)?[a-z_][\w.]*",
            r"\bdelete\s+from\s+[a-z_][\w.]*",
            r"\b(drop_table|drop_column|remove_column)\s*\(",
        ),
    ):
        return f"destructive DB/data change: {destructive_path}"
    if _matches_any(
        pr_metadata,
        (
            r"\bdestructive\b.{0,40}\b(db|database|data|migration)\b",
            r"\b(drop|delete|truncate)\b.{0,40}\b(table|column|data)\b",
        ),
    ):
        return "destructive DB/data change: PR metadata"

    pit_path = _first_matching_path(paths, _is_pit_rule_path)
    if pit_path is not None:
        return f"PIT rule change: {pit_path}"
    if _matches_any(
        pr_metadata,
        (
            r"\b(change|relax|remove|bypass|rewrite|alter)\b.{0,40}"
            r"\b(available_at|point-in-time|pit|asof_date|lookahead)\b",
        ),
    ):
        return "PIT rule change: PR metadata"

    feature_path = _first_matching_path(paths, _is_feature_semantics_path)
    feature_diff = _diff_for_path(
        feature_path,
        paths=paths,
        added_diff=added_diff,
        added_diff_by_path=added_diff_by_path,
    )
    if feature_path is not None and _semantic_diff_or_metadata(
        feature_diff,
        pr_metadata,
        (
            r"\bfeature(_definition)?\b",
            r"\blookback\b",
            r"\bwindow\b",
            r"\bpct_change\b",
            r"\brolling\b",
            r"\bmomentum\b",
            r"\bvolatility\b",
            r"\bdollar_volume\b",
            r"\breturn\b",
            r"\bmodel_version\b",
            r"\bprompt_version\b",
        ),
    ):
        return f"feature semantic change: {feature_path}"

    label_path = _first_matching_path(paths, _is_label_semantics_path)
    label_diff = _diff_for_path(
        label_path,
        paths=paths,
        added_diff=added_diff,
        added_diff_by_path=added_diff_by_path,
    )
    if label_path is not None and _semantic_diff_or_metadata(
        label_diff,
        pr_metadata,
        (
            r"\blabel\b",
            r"\bhorizon\b",
            r"\bhorizon_days\b",
            r"\bforward_return\b",
            r"\bexcess_return\b",
            r"\bcorporate_action\b",
            r"\bavailable_at\b",
        ),
    ):
        return f"label semantic change: {label_path}"

    backtest_path = _first_matching_path(paths, _is_backtest_semantics_path)
    backtest_diff = _diff_for_path(
        backtest_path,
        paths=paths,
        added_diff=added_diff,
        added_diff_by_path=added_diff_by_path,
    )
    if backtest_path is not None and _backtest_metric_semantics_changed(
        backtest_diff,
        pr_metadata,
    ):
        return f"backtest metric semantic change: {backtest_path}"

    secret_path = _first_matching_path(paths, _is_secret_handling_path)
    if secret_path is not None:
        return f"secret handling change: {secret_path}"
    if _metadata_says_secret_handling_changed(pr_metadata):
        return "secret handling change: PR metadata"

    external_path = _first_matching_path(paths, _is_external_call_path)
    external_diff = _diff_for_path(
        external_path,
        paths=paths,
        added_diff=added_diff,
        added_diff_by_path=added_diff_by_path,
    )
    if external_path is not None and _matches_any(
        "\n".join((external_diff, pr_metadata)),
        (
            r"\b(urllib\.request\.urlopen|requests\.|httpx\.|aiohttp\.)",
            r"https?://",
            r"\b(fmp|financialmodelingprep)\b",
            r"\b(paid|billing|live)\b.{0,40}\b(call|request|service|api)\b",
        ),
    ):
        return f"paid/live external call change: {external_path}"

    automation_path = _first_matching_path(paths, _is_automation_surface_path)
    automation_diff = _diff_for_path(
        automation_path,
        paths=paths,
        added_diff=added_diff,
        added_diff_by_path=added_diff_by_path,
    )
    if automation_path is not None and _matches_any(
        "\n".join((automation_diff, pr_metadata)),
        (
            r"\bactive_states\s*:[^\n]*(merging|safety review)\b",
            r"\bpermissions\s*:[^\n]*write-all\b",
            r"(?<!\S)--admin(?!\S)",
            r"\bforce[- ]?push\b",
            r"\bskip\b.{0,30}\brequired checks\b",
            r"\bbypass\b.{0,30}\b(checks|review|queue|safety)\b",
        ),
    ):
        return f"automation permission expansion: {automation_path}"

    return None


def planned_objective_safety_allowance(
    issue: LinearIssue,
    pr: PullRequest,
    safety_trigger: str,
) -> str | None:
    contract_allowance = planned_contract_safety_allowance(issue, pr, safety_trigger)
    if contract_allowance is not None:
        return contract_allowance
    security_allowance = planned_contract_security_hardening_allowance(
        issue,
        pr,
        safety_trigger,
    )
    if security_allowance is not None:
        return security_allowance
    return planned_semantic_safety_allowance(issue, pr, safety_trigger)


def planned_contract_safety_allowance(
    issue: LinearIssue,
    pr: PullRequest,
    safety_trigger: str,
) -> str | None:
    if issue_ticket_role(issue.description) != "contract":
        return None
    if not safety_trigger.startswith("PIT rule change:"):
        return None
    if "PR metadata" in safety_trigger:
        return None

    changed_files = tuple(pr.changed_files)
    if not changed_files:
        return None
    paths = tuple(
        normalized
        for changed_file in changed_files
        if (normalized := _normalize_path(changed_file.path))
    )
    if not paths or not all(_is_docs_artifact_path(path) for path in paths):
        return None
    if any(changed_file.deletions != 0 for changed_file in changed_files):
        return None

    owns = issue_section_paths(issue.description, "Owns")
    if not owns or not all(_path_is_covered(path, owns) for path in paths):
        return None

    do_not_touch = issue_section_paths(issue.description, "Do Not Touch")
    if any(_path_is_covered(path, do_not_touch) for path in paths):
        return None

    if pit_relaxation_signal(pr.diff or ""):
        return None

    return (
        "planned contract docs-only PIT clarification in ticket-owned paths "
        f"(original trigger: {safety_trigger})"
    )


def planned_contract_security_hardening_allowance(
    issue: LinearIssue,
    pr: PullRequest,
    safety_trigger: str,
) -> str | None:
    if issue_ticket_role(issue.description) != "contract":
        return None
    if not safety_trigger.startswith("secret handling change:"):
        return None
    if "PR metadata" in safety_trigger:
        return None

    changed_files = tuple(pr.changed_files)
    if not changed_files:
        return None
    paths = tuple(
        normalized
        for changed_file in changed_files
        if (normalized := _normalize_path(changed_file.path))
    )
    if not paths or not all(_is_docs_artifact_path(path) for path in paths):
        return None
    if not any(_is_secret_handling_path(path) for path in paths):
        return None
    if any(changed_file.deletions != 0 for changed_file in changed_files):
        return None

    owns = issue_section_paths(issue.description, "Owns")
    if not owns or not all(_path_is_covered(path, owns) for path in paths):
        return None

    do_not_touch = issue_section_paths(issue.description, "Do Not Touch")
    if any(_path_is_covered(path, do_not_touch) for path in paths):
        return None

    diff = pr.diff or ""
    if secret_relaxation_signal(diff):
        return None
    if not secret_hardening_signal(diff):
        return None

    return (
        "planned contract docs-only security hardening in ticket-owned paths "
        f"(original trigger: {safety_trigger})"
    )


def planned_semantic_safety_allowance(
    issue: LinearIssue,
    pr: PullRequest,
    safety_trigger: str,
) -> str | None:
    if issue_risk_class(issue.description) != "semantic":
        return None
    if issue_ticket_role(issue.description) not in {
        "contract",
        "implementation",
        "integration",
    }:
        return None
    if "PR metadata" in safety_trigger:
        return None
    if not safety_trigger.startswith(
        (
            "feature semantic change:",
            "label semantic change:",
            "backtest metric semantic change:",
        )
    ):
        return None

    changed_files = tuple(pr.changed_files)
    if not changed_files:
        return None
    paths = tuple(
        normalized
        for changed_file in changed_files
        if (normalized := _normalize_path(changed_file.path))
    )
    owns = issue_section_paths(issue.description, "Owns")
    if not owns or not paths or not all(_path_is_covered(path, owns) for path in paths):
        return None

    do_not_touch = issue_section_paths(issue.description, "Do Not Touch")
    if any(_path_is_covered(path, do_not_touch) for path in paths):
        return None

    return (
        "planned semantic change in ticket-owned paths "
        f"(original trigger: {safety_trigger})"
    )


def decision_reason(prefix: str | None, reason: str) -> str:
    if prefix is None:
        return reason
    return f"{prefix}; {reason}"


def issue_ticket_role(description: str) -> str:
    match = re.search(r"(?im)^Ticket Role:\s*`?(?P<role>[a-z_ -]+)`?\s*$", description)
    if match is None:
        return ""
    return match.group("role").strip().lower().replace(" ", "-")


def issue_risk_class(description: str) -> str:
    match = re.search(r"(?im)^Risk Class:\s*`?(?P<risk>[a-z_ -]+)`?\s*$", description)
    if match is None:
        return ""
    return match.group("risk").strip().lower().replace(" ", "-")


def issue_section_paths(description: str, heading: str) -> tuple[str, ...]:
    section = _markdown_section(description, heading)
    if not section:
        return ()
    return tuple(
        normalized
        for path in re.findall(r"`([^`]+)`", section)
        if (normalized := _normalize_path(path))
    )


def _is_docs_artifact_path(path: str) -> bool:
    return path.endswith((".md", ".rst", ".txt"))


def _path_is_covered(path: str, allowed_paths: Sequence[str]) -> bool:
    return any(_path_matches(path, allowed_path) for allowed_path in allowed_paths)


def pit_relaxation_signal(diff: str) -> bool:
    added_text = _added_diff_text(diff)
    if not added_text:
        return False
    return _matches_any(
        added_text,
        (
            r"\b(relax|bypass|disable|ignore|weaken|remove)\b.{0,80}"
            r"\b(available_at|point-in-time|pit|asof_date|lookahead)\b",
            r"\b(available_at|point-in-time|pit|asof_date|lookahead)\b.{0,80}"
            r"\b(optional|not required|no longer required|may be ignored)\b",
        ),
    )


def secret_hardening_signal(diff: str) -> bool:
    return any(
        _line_hardens_secret_handling(line)
        for line in _added_diff_text(diff).splitlines()
    )


def secret_relaxation_signal(diff: str) -> bool:
    for line in _added_diff_text(diff).splitlines():
        if not _line_mentions_secret(line):
            continue
        if _matches_any(
            line,
            (
                r"\b(redact|mask|scrub|omit|exclude|strip)\b.{0,50}"
                r"\b(optional|not required|no longer required|disabled|bypass)\b",
                r"\b(optional|not required|no longer required|disabled|bypass)\b"
                r".{0,50}\b(redact|mask|scrub|omit|exclude|strip)\b",
                r"\b(no longer|need not|do not need to)\b.{0,80}"
                r"\b(redact|mask|scrub|omit|exclude|strip)\b",
            ),
        ):
            return True
        if _matches_any(
            line,
            (
                r"\b(may|can|allow|allowed|optional)\b.{0,80}"
                r"\b(copy|copied|expose|exposed|log|logged|store|stored|"
                r"persist|persisted|write|written|commit|committed)\b",
                r"\b(copy|copied|expose|exposed|log|logged|store|stored|"
                r"persist|persisted|write|written|commit|committed)\b.{0,80}"
                r"\b(may|can|allow|allowed|optional)\b",
                r"\b(copy|copied|expose|exposed|log|logged|store|stored|"
                r"persist|persisted|write|written|commit|committed)\b.{0,80}"
                r"\b(secret|credential|api[_ -]?keys?|token)\b",
            ),
        ):
            return True
        if _line_hardens_secret_handling(line):
            continue
    return False


def _line_mentions_secret(line: str) -> bool:
    return _matches_any(
        line,
        (
            r"\b(secret|credential|api[_ -]?keys?|token)\b",
            r"\b(linear_api_key|fmp_api_key)\b",
        ),
    )


def _line_hardens_secret_handling(line: str) -> bool:
    if not _line_mentions_secret(line):
        return False
    return _matches_any(
        line,
        (
            r"\b(redact|redacted|mask|masked|scrub|scrubbed|omit|omitted|"
            r"exclude|excluded|remove|removed|strip|stripped)\b.{0,80}"
            r"\b(secret|credential|api[_ -]?keys?|token|linear_api_key|fmp_api_key)\b",
            r"\b(secret|credential|api[_ -]?keys?|token|linear_api_key|fmp_api_key)"
            r"\b.{0,80}\b(redact|redacted|mask|masked|scrub|scrubbed|omit|"
            r"omitted|exclude|excluded|remove|removed|strip|stripped)\b",
            r"\b(do not|does not|must not|never|without|no)\b.{0,80}"
            r"\b(log|store|persist|write|commit|expose|copy)\b.{0,80}"
            r"\b(secret|credential|api[_ -]?keys?|token|linear_api_key|fmp_api_key)\b",
            r"\b(secret|credential|api[_ -]?keys?|token|linear_api_key|fmp_api_key)"
            r"\b.{0,80}"
            r"\b(do not|does not|must not|never|without|no)\b.{0,80}"
            r"\b(log|store|persist|write|commit|expose|copy)\b",
        ),
    )


def build_safety_review_comment(
    *,
    issue: LinearIssue,
    pr: PullRequest,
    trigger: str,
) -> str:
    objective = parent_objective(issue.description)
    return (
        "## Safety Review Blocker\n\n"
        f"Objective: {objective or 'Not specified'}\n"
        f"Ticket: {issue.identifier} - {issue.title}\n"
        f"PR: #{pr.number} {pr.url}\n"
        f"Trigger: {trigger}\n\n"
        "Allowed next action: Michael reviews the exception. If the risk is "
        "explicitly accepted, move the issue back to `Merging`; otherwise move "
        "it to `Rework` with the requested repair. The merge steward must not "
        "queue this PR while it is in `Safety Review`."
    )


def parent_objective(description: str) -> str:
    match = re.search(r"(?im)^Parent Objective:\s*(?P<objective>.+?)\s*$", description)
    if match is None:
        return ""
    return match.group("objective").strip()


def _do_not_touch_trigger(description: str, paths: Sequence[str]) -> str | None:
    section = _markdown_section(description, "Do Not Touch")
    if not section:
        return None

    for protected_path in re.findall(r"`([^`]+)`", section):
        normalized = _normalize_path(protected_path)
        if normalized and any(_path_matches(path, normalized) for path in paths):
            return f"ticket scope drift: changed Do Not Touch path {protected_path}"

    lowered = section.lower()
    if "database schema" in lowered or "migrations" in lowered:
        path = _first_matching_path(paths, _is_db_or_data_path)
        if path is not None:
            return f"ticket scope drift: changed Do Not Touch database path {path}"
    if "secret" in lowered or "credential" in lowered:
        path = _first_matching_path(paths, _is_secret_handling_path)
        if path is not None:
            return f"ticket scope drift: changed Do Not Touch secret path {path}"

    return None


def _markdown_section(markdown: str, heading: str) -> str:
    pattern = (
        rf"(?ims)^(?:#+\s*)?{re.escape(heading)}\s*:?\s*$"
        rf"(?P<section>.*?)(?=^(?:#+\s*)?[A-Z][A-Za-z0-9 /`-]+:\s*$|\Z)"
    )
    match = re.search(pattern, markdown)
    if match is None:
        return ""
    return match.group("section").strip()


def _normalize_path(path: str) -> str:
    normalized = path.strip().replace("\\", "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized.lower()


def _path_matches(path: str, protected_path: str) -> bool:
    protected = protected_path.rstrip("/")
    return path == protected or path.startswith(f"{protected}/")


def _first_matching_path(
    paths: Sequence[str],
    predicate: "Callable[[str], bool]",
) -> str | None:
    for path in paths:
        if predicate(path):
            return path
    return None


def _added_diff_text(diff: str) -> str:
    lines: list[str] = []
    for line in diff.splitlines():
        if line.startswith("+++") or not line.startswith("+"):
            continue
        lines.append(line[1:])
    return "\n".join(lines)


def _added_diff_text_by_path(diff: str) -> Mapping[str, str]:
    lines_by_path: dict[str, list[str]] = {}
    current_path = ""
    for line in diff.splitlines():
        if line.startswith("diff --git "):
            match = re.match(r"diff --git a/(.*?) b/(?P<path>.+)$", line)
            current_path = _normalize_path(match.group("path")) if match else ""
            continue
        if line.startswith("+++ b/"):
            current_path = _normalize_path(line.removeprefix("+++ b/"))
            continue
        if not current_path or line.startswith("+++") or not line.startswith("+"):
            continue
        lines_by_path.setdefault(current_path, []).append(line[1:])
    return {path: "\n".join(lines) for path, lines in lines_by_path.items()}


def _diff_for_path(
    path: str | None,
    *,
    paths: Sequence[str],
    added_diff: str,
    added_diff_by_path: Mapping[str, str],
) -> str:
    if path is None:
        return ""
    path_diff = added_diff_by_path.get(path)
    if path_diff is not None:
        return path_diff
    if not added_diff_by_path or len(paths) == 1:
        return added_diff
    return ""


def _matches_any(text: str, patterns: Sequence[str]) -> bool:
    return any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns)


def _metadata_says_secret_handling_changed(text: str) -> bool:
    for line in text.splitlines():
        if not _matches_any(
            line,
            (
                r"\b(secret|credential|api[_ -]?key|token)\b",
                r"\b(linear_api_key|fmp_api_key)\b",
            ),
        ):
            continue
        if _matches_any(
            line,
            (
                r"\b(do not|does not|did not|no|not|never|without)\b.{0,50}"
                r"\b(secret|credential|api[_ -]?key|token)\b",
                r"\b(secret|credential|api[_ -]?key|token)\b.{0,50}"
                r"\b(no|not|none|absent)\b",
            ),
        ):
            continue
        if _matches_any(
            line,
            (
                r"\b(secret|credential|api[_ -]?key|token)\b.{0,50}"
                r"\b(add|change|copy|expose|handle|log|read|rotate|store|update|write)\b",
                r"\b(add|change|copy|expose|handle|log|read|rotate|store|update|write)\b"
                r".{0,50}\b(secret|credential|api[_ -]?key|token)\b",
                r"\b(linear_api_key|fmp_api_key)\b",
            ),
        ):
            return True
    return False


def _backtest_metric_semantics_changed(added_diff: str, pr_metadata: str) -> bool:
    if not added_diff:
        return _matches_any(
            pr_metadata,
            (
                r"\b(change|alter|replace|rewrite|redefine)\b.{0,80}"
                r"\b(metric|sharpe|drawdown|turnover|baseline|cost|return)\b",
                r"\b(metric|sharpe|drawdown|turnover|baseline|cost|return)\b.{0,80}"
                r"\b(change|alter|replace|rewrite|redefine)\b",
            ),
        )

    signal = "\n".join((added_diff, pr_metadata))
    return _matches_any(
        signal,
        (
            r"\b(metrics?|baseline_metrics|label_scramble_metrics)\b\s*(\[|=|\.)",
            r"\b(sharpe|drawdown|turnover|gross|net|return)\b.{0,80}"
            r"(=|\.mean\(|\.sum\(|pct_change\(|cumprod\(|cumsum\()",
            r"(=|\.mean\(|\.sum\(|pct_change\(|cumprod\(|cumsum\().{0,80}"
            r"\b(sharpe|drawdown|turnover|gross|net|return)\b",
            r"\b(change|alter|replace|rewrite|redefine|annualize|normalize)\b.{0,80}"
            r"\b(metric|sharpe|drawdown|turnover|baseline|cost|return)\b",
            r"\b(metric|sharpe|drawdown|turnover|baseline|cost|return)\b.{0,80}"
            r"\b(change|alter|replace|rewrite|redefine|annualize|normalize)\b",
        ),
    )


def _semantic_diff_or_metadata(
    added_diff: str,
    pr_metadata: str,
    patterns: Sequence[str],
) -> bool:
    if not added_diff:
        return True
    return _matches_any("\n".join((added_diff, pr_metadata)), patterns)


def _is_db_or_data_path(path: str) -> bool:
    return (
        path.startswith("db/")
        or path.endswith(".sql")
        or path.startswith("data/")
        or path.startswith("scripts/apply_migrations.py")
        or path.startswith("scripts/bootstrap_database.py")
        or path.startswith("scripts/seed_")
    )


def _is_pit_rule_path(path: str) -> bool:
    return path in {
        "docs/pit_discipline.md",
        "config/available_at_policies.yaml",
        "src/silver/time/available_at_policies.py",
        "scripts/seed_available_at_policies.py",
    }


def _is_feature_semantics_path(path: str) -> bool:
    return path.startswith("src/silver/features/")


def _is_label_semantics_path(path: str) -> bool:
    return path.startswith("src/silver/labels/")


def _is_backtest_semantics_path(path: str) -> bool:
    return (
        path.startswith("src/silver/backtest/")
        or path in {
            "scripts/run_falsifier.py",
            "scripts/check_falsifier_inputs.py",
        }
    )


def _is_secret_handling_path(path: str) -> bool:
    name = Path(path).name
    return (
        path in {".env", ".env.example", "docs/security.md"}
        or "secret" in path
        or "credential" in path
        or name.endswith(".key")
    )


def _is_external_call_path(path: str) -> bool:
    return (
        path.startswith("src/silver/sources/")
        or path.startswith("src/silver/ingest/")
        or path.startswith("scripts/ingest_")
        or path == "scripts/run_phase1_pipeline.py"
    )


def _is_automation_surface_path(path: str) -> bool:
    return (
        path == "workflow.md"
        or path.startswith(".github/workflows/")
        or path in {
            "docs/symphony.md",
            "scripts/merge_steward.py",
            "scripts/planning_steward.py",
        }
    )


def required_check_status(
    pr: PullRequest,
    required_checks: Sequence[str],
) -> Decision:
    checks_by_name = {check.name: check for check in pr.checks}
    missing = [name for name in required_checks if name not in checks_by_name]
    if missing:
        return Decision(
            "wait",
            "missing required check(s): " + ", ".join(sorted(missing)),
            pr.number,
        )

    pending: list[str] = []
    failed: list[str] = []
    for name in required_checks:
        check = checks_by_name[name]
        status = (check.status or "").upper()
        conclusion = (check.conclusion or "").upper()
        if status != "COMPLETED":
            pending.append(name)
        elif conclusion != "SUCCESS":
            failed.append(f"{name}={conclusion or 'UNKNOWN'}")

    if failed:
        return Decision(
            "move_rework",
            "failed required check(s): " + ", ".join(failed),
            pr.number,
        )
    if pending:
        return Decision(
            "wait",
            "pending required check(s): " + ", ".join(sorted(pending)),
            pr.number,
        )
    return Decision("queue", "required checks passed", pr.number)


def merging_issue_candidates(
    issues: Sequence[LinearIssue],
) -> tuple[LinearIssue, ...]:
    return tuple(issue for issue in issues if issue.state == MERGING_STATE)


def stale_reconciliation_candidates(
    issues: Sequence[LinearIssue],
) -> tuple[LinearIssue, ...]:
    terminal_states = {state.lower() for state in TERMINAL_STATE_NAMES}
    return tuple(
        issue
        for issue in issues
        if issue.state != MERGING_STATE and issue.state.lower() not in terminal_states
    )


def choose_pr_for_issue(
    issue: LinearIssue,
    pull_requests: Sequence[PullRequest],
) -> PullRequest | None:
    matches = [
        pr
        for pr in pull_requests
        if pr_identity_matches_issue(issue.identifier, pr)
    ]
    if not matches:
        return None

    open_matches = [pr for pr in matches if pr.state == "OPEN"]
    if open_matches:
        return max(open_matches, key=lambda pr: pr.number)
    return max(matches, key=lambda pr: pr.number)


def pr_identity_matches_issue(identifier: str, pr: PullRequest) -> bool:
    # PR bodies can contain proof-packet audit text for other tickets, so only
    # identity-bearing fields are safe for issue matching.
    identity_text = "\n".join((pr.title, pr.head_ref_name))
    return _contains_issue_token(identity_text, identifier)


def _contains_issue_token(text: str, identifier: str) -> bool:
    escaped = re.escape(identifier)
    return bool(re.search(rf"(?<![a-z0-9]){escaped}(?![a-z0-9])", text, re.I))


def format_decision(
    issue: LinearIssue,
    decision: Decision,
    pr: PullRequest | None,
) -> str:
    pr_display = f"PR #{pr.number}" if pr else "no PR"
    return (
        f"{issue.identifier} | {decision.action} | {pr_display} | "
        f"{decision.reason}"
    )


def check_configuration(
    *,
    args: argparse.Namespace,
    required_checks: Sequence[str],
) -> str:
    lines = ["Merge steward configuration check", ""]
    if not args.project:
        raise MergeStewardError("Linear project is not configured")
    if not args.repo:
        raise MergeStewardError("GitHub repo is not configured")
    if shutil.which("gh") is None:
        raise MergeStewardError("GitHub CLI `gh` is required")

    lines.append(f"Linear project: {args.project}")
    lines.append(f"GitHub repo: {args.repo}")
    lines.append("Required checks: " + ", ".join(required_checks))
    lines.append("Result: local merge steward configuration is valid")
    return "\n".join(lines)


def effective_watch_poll_interval(poll_interval: int) -> int:
    if isinstance(poll_interval, bool) or poll_interval < 1:
        raise MergeStewardError("poll_interval must be a positive integer")
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
    return str(data.get("tracker", {}).get("project_slug", ""))


def detect_github_repo() -> str:
    result = subprocess.run(
        ["git", "config", "--get", "remote.origin.url"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        return ""
    return parse_github_repo(result.stdout.strip())


def parse_github_repo(remote_url: str) -> str:
    if not remote_url:
        return ""
    patterns = (
        r"github\.com[:/](?P<repo>[^/]+/[^/.]+)(?:\.git)?$",
        r"github\.com/(?P<repo>[^/]+/[^/.]+)(?:\.git)?$",
    )
    for pattern in patterns:
        match = re.search(pattern, remote_url)
        if match:
            return match.group("repo")
    return ""


def _parse_pull_request(
    item: Mapping[str, Any],
    queue_map: Mapping[int, bool],
) -> PullRequest:
    number = int(item["number"])
    changed_files = tuple(
        changed_file
        for file_item in item.get("files") or ()
        if (changed_file := _parse_changed_file(file_item)) is not None
    )
    checks = tuple(
        CheckRun(
            name=str(check.get("name", "")),
            status=check.get("status"),
            conclusion=check.get("conclusion"),
        )
        for check in item.get("statusCheckRollup") or ()
        if check.get("name")
    )
    return PullRequest(
        number=number,
        title=str(item.get("title") or ""),
        url=str(item.get("url") or ""),
        state=str(item.get("state") or ""),
        head_ref_name=str(item.get("headRefName") or ""),
        body=str(item.get("body") or ""),
        merged_at=item.get("mergedAt"),
        is_draft=bool(item.get("isDraft")),
        merge_state_status=item.get("mergeStateStatus"),
        auto_merge_enabled=item.get("autoMergeRequest") is not None,
        in_merge_queue=bool(queue_map.get(number, False)),
        changed_files=changed_files,
        diff=None,
        checks=checks,
    )


def _parse_changed_file(item: object) -> ChangedFile | None:
    if isinstance(item, str):
        path = item
        additions = None
        deletions = None
    elif isinstance(item, Mapping):
        path = str(item.get("path") or item.get("filename") or "")
        additions = _optional_int(item.get("additions"))
        deletions = _optional_int(item.get("deletions"))
    else:
        return None

    if not path:
        return None
    return ChangedFile(path=path, additions=additions, deletions=deletions)


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


if __name__ == "__main__":
    raise SystemExit(main())
