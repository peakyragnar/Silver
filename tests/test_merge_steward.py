from __future__ import annotations

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


def _issue(identifier: str):
    return merge_steward.LinearIssue(
        id="issue-id",
        identifier=identifier,
        title="Test issue",
        url=f"https://linear.app/arrow1/issue/{identifier.lower()}/test",
        state="Merging",
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
