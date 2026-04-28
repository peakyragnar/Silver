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
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import yaml


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_PATH = ROOT / "WORKFLOW.md"
DEFAULT_REQUIRED_CHECKS = ("Python 3.10 checks",)

IssueAction = Literal["queue", "mark_done", "move_rework", "wait", "skip"]


class MergeStewardError(RuntimeError):
    """Raised when the merge steward cannot safely continue."""


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
    state: str
    team_states: Mapping[str, LinearState]


@dataclass(frozen=True, slots=True)
class CheckRun:
    name: str
    status: str | None
    conclusion: str | None


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
    checks: tuple[CheckRun, ...]


@dataclass(frozen=True, slots=True)
class Decision:
    action: IssueAction
    reason: str
    pr_number: int | None = None


class LinearClient:
    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

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
            raise MergeStewardError(
                f"Linear request failed with HTTP {exc.code}: {detail}"
            ) from exc
        except urllib.error.URLError as exc:
            raise MergeStewardError(f"Linear request failed: {exc}") from exc

        if payload.get("errors"):
            raise MergeStewardError(f"Linear returned errors: {payload['errors']}")
        return payload["data"]

    def merging_issues(self, project_id_or_slug: str) -> tuple[LinearIssue, ...]:
        query = """
        query($project: String!) {
          project(id: $project) {
            issues(first: 100) {
              nodes {
                id
                identifier
                title
                url
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

        merging_nodes = [
            node
            for node in project["issues"]["nodes"]
            if node["state"]["name"] == "Merging"
        ]
        if not merging_nodes:
            return ()

        states_by_team = {
            team_id: self.team_states(team_id)
            for team_id in {node["team"]["id"] for node in merging_nodes}
        }

        issues: list[LinearIssue] = []
        for node in merging_nodes:
            state = node["state"]["name"]
            issues.append(
                LinearIssue(
                    id=node["id"],
                    identifier=node["identifier"],
                    title=node["title"],
                    url=node["url"],
                    state=state,
                    team_states=states_by_team[node["team"]["id"]],
                )
            )
        return tuple(issues)

    def team_states(self, team_id: str) -> Mapping[str, LinearState]:
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
        return {
            node["name"]: LinearState(id=node["id"], name=node["name"])
            for node in team["states"]["nodes"]
        }

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
                "mergeStateStatus,autoMergeRequest,statusCheckRollup"
            ),
        ]
        payload = self.runner.json(command)
        queue_map = self._merge_queue_map(
            [int(item["number"]) for item in payload if item["state"] == "OPEN"]
        )
        return tuple(_parse_pull_request(item, queue_map) for item in payload)

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
        default=30,
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
    while True:
        run_once(
            args=args,
            linear=linear,
            github=github,
            required_checks=required_checks,
        )
        time.sleep(args.poll_interval)


def run_once(
    *,
    args: argparse.Namespace,
    linear: LinearClient,
    github: GitHubClient,
    required_checks: Sequence[str],
) -> None:
    issues = linear.merging_issues(args.project)
    if not issues:
        print("No Linear issues in Merging.")
        return

    pull_requests = github.list_pull_requests(args.limit)
    for issue in issues:
        pr = choose_pr_for_issue(issue, pull_requests)
        decision = decide_issue_action(issue, pr, required_checks)
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

    if decision.action == "mark_done":
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

    check_status = required_check_status(pr, required_checks)
    if check_status.action == "move_rework":
        return Decision("move_rework", check_status.reason, pr.number)
    if check_status.action == "wait":
        return Decision("wait", check_status.reason, pr.number)

    merge_state = (pr.merge_state_status or "UNKNOWN").upper()
    if merge_state == "DIRTY":
        return Decision("move_rework", "PR has merge conflicts", pr.number)
    if merge_state in {"BLOCKED", "UNKNOWN"}:
        return Decision("wait", f"PR merge state is {merge_state}", pr.number)
    if pr.in_merge_queue:
        return Decision("wait", "PR is already in GitHub merge queue", pr.number)
    if pr.auto_merge_enabled:
        return Decision("wait", "PR auto-merge is already enabled", pr.number)

    return Decision("queue", "PR is approved, green, and mergeable", pr.number)


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


def choose_pr_for_issue(
    issue: LinearIssue,
    pull_requests: Sequence[PullRequest],
) -> PullRequest | None:
    identifier = issue.identifier.lower()
    matches = [
        pr
        for pr in pull_requests
        if identifier in " ".join((pr.title, pr.head_ref_name, pr.body)).lower()
    ]
    if not matches:
        return None

    open_matches = [pr for pr in matches if pr.state == "OPEN"]
    if open_matches:
        return max(open_matches, key=lambda pr: pr.number)
    return max(matches, key=lambda pr: pr.number)


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
        checks=checks,
    )


if __name__ == "__main__":
    raise SystemExit(main())
