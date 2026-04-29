from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MERGE_STEWARD_SCRIPT = ROOT / "scripts" / "merge_steward.py"


def load_merge_steward_module():
    spec = importlib.util.spec_from_file_location(
        "merge_steward",
        MERGE_STEWARD_SCRIPT,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


merge_steward = load_merge_steward_module()


def test_merged_pr_marks_issue_done() -> None:
    issue = _issue("ARR-26")
    pr = _pr(26, state="MERGED", merged_at="2026-04-28T21:27:39Z")

    decision = merge_steward.decide_issue_action(
        issue,
        pr,
        ("Python 3.10 checks",),
    )

    assert decision.action == "mark_done"
    assert decision.pr_number == 26


def test_stale_merged_pr_reconciles_nonterminal_issue() -> None:
    issue = _issue("ARR-36", state="In Progress")
    pr = _pr(
        36,
        title="ARR-36 Add post-Todo reset guard",
        state="MERGED",
        merged_at="2026-04-28T21:27:39Z",
    )

    decision = merge_steward.decide_stale_issue_action(issue, pr)

    assert decision.action == "stale_mark_done"
    assert decision.pr_number == 36
    assert "stale" in decision.reason


def test_stale_open_pr_is_left_unchanged() -> None:
    issue = _issue("ARR-37", state="Rework")
    pr = _pr(
        37,
        title="ARR-37 Add reset workflow contract",
        state="OPEN",
    )

    decision = merge_steward.decide_stale_issue_action(issue, pr)

    assert decision.action == "skip"
    assert decision.pr_number == 37
    assert "not merged" in decision.reason


def test_stale_missing_pr_is_left_unchanged() -> None:
    issue = _issue("ARR-38", state="Todo")

    decision = merge_steward.decide_stale_issue_action(issue, None)

    assert decision.action == "skip"
    assert decision.pr_number is None
    assert "no matching pull request" in decision.reason


def test_terminal_states_are_not_stale_reconciliation_candidates() -> None:
    issues = (
        _issue("ARR-40", state="Todo"),
        _issue("ARR-41", state="Done"),
        _issue("ARR-42", state="Canceled"),
        _issue("ARR-43", state="Duplicate"),
        _issue("ARR-44", state="Merging"),
    )

    candidates = merge_steward.stale_reconciliation_candidates(issues)

    assert [issue.identifier for issue in candidates] == ["ARR-40"]


def test_dry_run_reports_stale_reconciliation_without_writes(capsys) -> None:
    linear = _FakeLinear(
        issues=(_issue("ARR-36", state="In Progress"),),
    )
    github = _FakeGitHub(
        pull_requests=(
            _pr(
                36,
                title="ARR-36 Add post-Todo reset guard",
                state="MERGED",
                merged_at="2026-04-28T21:27:39Z",
            ),
        ),
    )
    args = argparse.Namespace(project="silver", limit=100, dry_run=True)

    merge_steward.run_once(
        args=args,
        linear=linear,
        github=github,
        required_checks=("Python 3.10 checks",),
    )

    output = capsys.readouterr().out
    assert "ARR-36 | stale_mark_done | PR #36" in output
    assert "stale" in output
    assert linear.comments == []
    assert linear.state_updates == []


def test_stale_mark_done_uses_standard_merge_confirmation() -> None:
    issue = _issue("ARR-36", state="In Progress")
    pr = _pr(
        36,
        title="ARR-36 Add post-Todo reset guard",
        state="MERGED",
        merged_at="2026-04-28T21:27:39Z",
    )
    linear = _FakeLinear()
    github = _FakeGitHub()
    decision = merge_steward.Decision(
        "stale_mark_done",
        "stale nonterminal issue has merged PR",
        36,
    )

    merge_steward.apply_decision(
        issue=issue,
        pr=pr,
        decision=decision,
        linear=linear,
        github=github,
    )

    assert len(linear.comments) == 1
    assert linear.comments[0][0] == "issue-id"
    assert "## Merge Confirmation" in linear.comments[0][1]
    assert pr.url in linear.comments[0][1]
    assert linear.state_updates == [("issue-id", "done-id")]


def test_green_clean_open_pr_is_queued() -> None:
    issue = _issue("ARR-27")
    pr = _pr(
        28,
        title="ARR-27 Add falsifier input coverage diagnostics",
        merge_state_status="CLEAN",
    )

    decision = merge_steward.decide_issue_action(
        issue,
        pr,
        ("Python 3.10 checks",),
    )

    assert decision.action == "queue"
    assert "green" in decision.reason


def test_merge_queue_pr_waits_without_requeueing() -> None:
    issue = _issue("ARR-27")
    pr = _pr(
        28,
        title="ARR-27 Add falsifier input coverage diagnostics",
        merge_state_status="CLEAN",
        in_merge_queue=True,
    )

    decision = merge_steward.decide_issue_action(
        issue,
        pr,
        ("Python 3.10 checks",),
    )

    assert decision.action == "wait"
    assert "merge queue" in decision.reason


def test_failed_required_check_moves_to_rework() -> None:
    issue = _issue("ARR-28")
    pr = _pr(
        27,
        title="ARR-28 Add label-scramble falsifier module",
        checks=(
            merge_steward.CheckRun(
                name="Python 3.10 checks",
                status="COMPLETED",
                conclusion="FAILURE",
            ),
        ),
    )

    decision = merge_steward.decide_issue_action(
        issue,
        pr,
        ("Python 3.10 checks",),
    )

    assert decision.action == "move_rework"
    assert "failed required check" in decision.reason


def test_dirty_pr_moves_to_rework() -> None:
    issue = _issue("ARR-29")
    pr = _pr(
        25,
        title="ARR-29 Add regime slicing utilities",
        merge_state_status="DIRTY",
    )

    decision = merge_steward.decide_issue_action(
        issue,
        pr,
        ("Python 3.10 checks",),
    )

    assert decision.action == "move_rework"
    assert "merge conflicts" in decision.reason


def test_pending_required_check_waits() -> None:
    issue = _issue("ARR-30")
    pr = _pr(
        24,
        title="ARR-30 Add Phase 1 local runbook and preflight",
        checks=(
            merge_steward.CheckRun(
                name="Python 3.10 checks",
                status="IN_PROGRESS",
                conclusion=None,
            ),
        ),
    )

    decision = merge_steward.decide_issue_action(
        issue,
        pr,
        ("Python 3.10 checks",),
    )

    assert decision.action == "wait"
    assert "pending" in decision.reason


def test_choose_pr_prefers_open_highest_number_match() -> None:
    issue = _issue("ARR-24")
    pull_requests = (
        _pr(
            18,
            title="ARR-24 Add walk-forward momentum backtest runner",
            state="MERGED",
            merged_at="2026-04-28T20:55:43Z",
        ),
        _pr(
            30,
            title="ARR-24 Follow-up",
            head_ref_name="arr-24-follow-up",
            state="OPEN",
        ),
        _pr(
            29,
            title="ARR-24 Older follow-up",
            head_ref_name="arr-24-older-follow-up",
            state="OPEN",
        ),
    )

    pr = merge_steward.choose_pr_for_issue(issue, pull_requests)

    assert pr is not None
    assert pr.number == 30


def test_parse_github_repo_handles_https_and_ssh_remotes() -> None:
    assert (
        merge_steward.parse_github_repo("https://github.com/SilverEnv/Silver.git")
        == "SilverEnv/Silver"
    )
    assert (
        merge_steward.parse_github_repo("git@github.com:SilverEnv/Silver.git")
        == "SilverEnv/Silver"
    )


def _issue(identifier: str, *, state: str = "Merging"):
    return merge_steward.LinearIssue(
        id="issue-id",
        identifier=identifier,
        title="Test issue",
        url=f"https://linear.app/arrow1/issue/{identifier.lower()}/test",
        state=state,
        team_states={
            "Done": merge_steward.LinearState(id="done-id", name="Done"),
            "Rework": merge_steward.LinearState(id="rework-id", name="Rework"),
        },
    )


def _pr(
    number: int,
    *,
    title: str | None = None,
    state: str = "OPEN",
    head_ref_name: str | None = None,
    body: str = "",
    merged_at: str | None = None,
    is_draft: bool = False,
    merge_state_status: str = "CLEAN",
    auto_merge_enabled: bool = False,
    in_merge_queue: bool = False,
    checks: tuple[object, ...] | None = None,
):
    return merge_steward.PullRequest(
        number=number,
        title=title or f"ARR-{number} Test PR",
        url=f"https://github.com/SilverEnv/Silver/pull/{number}",
        state=state,
        head_ref_name=head_ref_name or f"arr-{number}-test",
        body=body,
        merged_at=merged_at,
        is_draft=is_draft,
        merge_state_status=merge_state_status,
        auto_merge_enabled=auto_merge_enabled,
        in_merge_queue=in_merge_queue,
        checks=checks
        or (
            merge_steward.CheckRun(
                name="Python 3.10 checks",
                status="COMPLETED",
                conclusion="SUCCESS",
            ),
        ),
    )


class _FakeLinear:
    def __init__(self, issues=()) -> None:
        self.issues = tuple(issues)
        self.comments: list[tuple[str, str]] = []
        self.state_updates: list[tuple[str, str]] = []

    def project_issues(self, project_id_or_slug: str):
        assert project_id_or_slug == "silver"
        return self.issues

    def create_comment(self, issue_id: str, body: str) -> None:
        self.comments.append((issue_id, body))

    def update_issue_state(self, issue_id: str, state_id: str) -> None:
        self.state_updates.append((issue_id, state_id))


class _FakeGitHub:
    def __init__(self, pull_requests=()) -> None:
        self.pull_requests = tuple(pull_requests)
        self.queued: list[int] = []

    def list_pull_requests(self, limit: int):
        assert limit == 100
        return self.pull_requests

    def queue_pull_request(self, number: int) -> None:
        self.queued.append(number)
