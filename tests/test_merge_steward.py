from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

import pytest


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


@pytest.mark.parametrize(
    ("changed_files", "diff", "expected_reason"),
    (
        (
            ("db/migrations/005_drop_predictions.sql",),
            "+DROP TABLE silver.predictions;",
            "destructive",
        ),
        (
            ("docs/PIT_DISCIPLINE.md",),
            "+available_at is now event_at for backfilled facts",
            "PIT",
        ),
        (
            ("src/silver/features/momentum_12_1.py",),
            "+return adjusted_close.pct_change(63)",
            "feature",
        ),
        (
            ("src/silver/labels/forward_returns.py",),
            "+horizon_days = 42",
            "label",
        ),
        (
            ("src/silver/backtest/momentum_runner.py",),
            "+metrics[\"sharpe\"] = gross_return.mean()",
            "backtest",
        ),
        (
            ("docs/SECURITY.md",),
            "+FMP_API_KEY may be copied into local test fixtures",
            "secret",
        ),
        (
            ("src/silver/sources/fmp/client.py",),
            '+urllib.request.urlopen("https://financialmodelingprep.com/api/v3/quote/AAPL")',
            "paid/live",
        ),
        (
            ("WORKFLOW.md",),
            "+active_states: [Todo, In Progress, Rework, Merging]",
            "automation permission",
        ),
    ),
)
def test_safety_review_risks_move_to_safety_review(
    changed_files: tuple[str, ...],
    diff: str,
    expected_reason: str,
) -> None:
    issue = _issue("ARR-40")
    pr = _pr(
        40,
        title="ARR-40 Risky change",
        changed_files=_changed_files(*changed_files),
        diff=diff,
    )

    decision = merge_steward.decide_issue_action(
        issue,
        pr,
        ("Python 3.10 checks",),
    )

    assert decision.action == "move_safety_review"
    assert decision.pr_number == 40
    assert expected_reason in decision.reason


def test_planned_contract_docs_only_pit_change_queues() -> None:
    issue = _issue(
        "ARR-56",
        description=(
            "Parent Objective: fix-falsifier-reproducibility-evidence\n"
            "Ticket Role: contract\n\n"
            "Owns:\n"
            "- `SPEC.md`\n"
            "- `docs/PIT_DISCIPLINE.md`\n\n"
            "Do Not Touch:\n"
            "- `scripts/run_falsifier.py`\n"
        ),
    )
    pr = _pr(
        64,
        title="ARR-56 Define falsifier reproducibility evidence contract",
        changed_files=_changed_files("SPEC.md", "docs/PIT_DISCIPLINE.md"),
        diff=(
            "diff --git a/docs/PIT_DISCIPLINE.md b/docs/PIT_DISCIPLINE.md\n"
            "+++ b/docs/PIT_DISCIPLINE.md\n"
            "+Falsifier evidence must use feature values visible at each asof_date.\n"
            "diff --git a/SPEC.md b/SPEC.md\n"
            "+++ b/SPEC.md\n"
            "+Reports must expose available-at policy versions.\n"
        ),
    )

    decision = merge_steward.decide_issue_action(
        issue,
        pr,
        ("Python 3.10 checks",),
    )

    assert decision.action == "queue"
    assert "planned contract docs-only PIT clarification" in decision.reason
    assert "green" in decision.reason


def test_contract_pit_doc_deletion_still_requires_safety_review() -> None:
    issue = _issue(
        "ARR-56",
        description=(
            "Ticket Role: contract\n\n"
            "Owns:\n"
            "- `docs/PIT_DISCIPLINE.md`\n"
        ),
    )
    pr = _pr(
        64,
        title="ARR-56 Rewrite PIT contract",
        changed_files=(
            merge_steward.ChangedFile(
                path="docs/PIT_DISCIPLINE.md",
                additions=1,
                deletions=1,
            ),
        ),
        diff="+available_at is now optional for review-only reports\n",
    )

    decision = merge_steward.decide_issue_action(
        issue,
        pr,
        ("Python 3.10 checks",),
    )

    assert decision.action == "move_safety_review"
    assert "PIT rule change" in decision.reason


def test_routine_docs_and_tests_still_queue() -> None:
    issue = _issue("ARR-41")
    pr = _pr(
        41,
        title="ARR-41 Fix typos and test names",
        changed_files=_changed_files("README.md", "tests/test_merge_steward.py"),
        diff="+Fix typo in local runbook prose\n+def test_renamed_case():",
    )

    decision = merge_steward.decide_issue_action(
        issue,
        pr,
        ("Python 3.10 checks",),
    )

    assert decision.action == "queue"
    assert "green" in decision.reason


def test_negative_secret_checklist_text_does_not_trigger_safety_review() -> None:
    issue = _issue("ARR-48")
    pr = _pr(
        42,
        title="ARR-48 Tighten merge steward PR matching",
        changed_files=_changed_files("scripts/merge_steward.py"),
        body="- [x] Does not commit secrets or local data",
    )

    decision = merge_steward.decide_issue_action(
        issue,
        pr,
        ("Python 3.10 checks",),
    )

    assert decision.action == "queue"
    assert "green" in decision.reason


def test_affirmative_secret_metadata_triggers_safety_review() -> None:
    issue = _issue("ARR-49")
    pr = _pr(
        49,
        title="ARR-49 Update credential handling",
        changed_files=_changed_files("README.md"),
        body="Store API token material for integration tests.",
    )

    decision = merge_steward.decide_issue_action(
        issue,
        pr,
        ("Python 3.10 checks",),
    )

    assert decision.action == "move_safety_review"
    assert "secret handling" in decision.reason


def test_report_identity_plumbing_does_not_trigger_backtest_semantic_review() -> None:
    issue = _issue("ARR-45")
    pr = _pr(
        44,
        title="ARR-45 Surface persisted run identity in falsifier reports",
        changed_files=_changed_files(
            "scripts/run_falsifier.py",
            "src/silver/reports/falsifier.py",
        ),
        diff=(
            "+run_identity = FalsifierRunIdentity(\n"
            "+    model_run_id=row['model_run_id'],\n"
            "+    backtest_run_key=row['backtest_run_key'],\n"
            "+)\n"
        ),
    )

    decision = merge_steward.decide_issue_action(
        issue,
        pr,
        ("Python 3.10 checks",),
    )

    assert decision.action == "queue"
    assert "green" in decision.reason


def test_metadata_repository_helpers_do_not_trigger_backtest_semantic_review() -> None:
    issue = _issue("ARR-42")
    pr = _pr(
        45,
        title="ARR-42 Add backtest metadata repository helpers",
        changed_files=_changed_files(
            "src/silver/analytics/__init__.py",
            "src/silver/analytics/repository.py",
        ),
        diff=(
            "+class BacktestMetadataRepository:\n"
            "+    baseline_metrics: Mapping[str, object]\n"
            "+    label_scramble_metrics: Mapping[str, object]\n"
        ),
    )

    decision = merge_steward.decide_issue_action(
        issue,
        pr,
        ("Python 3.10 checks",),
    )

    assert decision.action == "queue"
    assert "green" in decision.reason


def test_mechanical_steward_fixes_still_queue() -> None:
    issue = _issue("ARR-42")
    pr = _pr(
        42,
        title="ARR-42 Format merge steward helper",
        changed_files=_changed_files("scripts/merge_steward.py"),
        diff="+    return format_decision(issue, decision, pr)",
    )

    decision = merge_steward.decide_issue_action(
        issue,
        pr,
        ("Python 3.10 checks",),
    )

    assert decision.action == "queue"
    assert "green" in decision.reason


def test_risky_test_fixture_text_does_not_block_mechanical_fix() -> None:
    issue = _issue("ARR-42")
    pr = _pr(
        42,
        title="ARR-42 Format merge steward helper",
        changed_files=_changed_files(
            "scripts/merge_steward.py",
            "tests/test_merge_steward.py",
        ),
        diff=(
            "diff --git a/tests/test_merge_steward.py b/tests/test_merge_steward.py\n"
            "+++ b/tests/test_merge_steward.py\n"
            "@@\n"
            "+active_states: [Todo, In Progress, Rework, Merging]\n"
            "diff --git a/scripts/merge_steward.py b/scripts/merge_steward.py\n"
            "+++ b/scripts/merge_steward.py\n"
            "@@\n"
            "+    return format_decision(issue, decision, pr)\n"
        ),
    )

    decision = merge_steward.decide_issue_action(
        issue,
        pr,
        ("Python 3.10 checks",),
    )

    assert decision.action == "queue"
    assert "green" in decision.reason


def test_do_not_touch_issue_text_routes_scope_drift_to_safety_review() -> None:
    issue = _issue(
        "ARR-40",
        description=(
            "Do Not Touch:\n\n"
            "* `scripts/planning_steward.py`\n\n"
            "Dependencies:\n\n"
            "* ARR-39\n"
        ),
    )
    pr = _pr(
        40,
        title="ARR-40 Touch unrelated steward",
        changed_files=_changed_files("scripts/planning_steward.py"),
        diff="+active_states: [Todo, In Progress, Rework]",
    )

    decision = merge_steward.decide_issue_action(
        issue,
        pr,
        ("Python 3.10 checks",),
    )

    assert decision.action == "move_safety_review"
    assert "scope drift" in decision.reason
    assert "scripts/planning_steward.py" in decision.reason


def test_safety_review_comment_includes_audit_fields() -> None:
    issue = _issue(
        "ARR-40",
        title="Add merge steward Safety Review gate",
        description="Parent Objective: autonomous-post-todo-operation\n",
    )
    pr = _pr(
        40,
        title="ARR-40 Add merge steward Safety Review gate",
        changed_files=_changed_files("docs/PIT_DISCIPLINE.md"),
        diff="+available_at now follows event_at",
    )
    linear = _FakeLinear()
    github = _FakeGitHub()
    decision = merge_steward.Decision(
        "move_safety_review",
        "PIT rule change: docs/PIT_DISCIPLINE.md",
        40,
    )

    merge_steward.apply_decision(
        issue=issue,
        pr=pr,
        decision=decision,
        linear=linear,
        github=github,
    )

    assert len(linear.comments) == 1
    comment = linear.comments[0][1]
    assert "Objective: autonomous-post-todo-operation" in comment
    assert "Ticket: ARR-40 - Add merge steward Safety Review gate" in comment
    assert "PR: #40 https://github.com/SilverEnv/Silver/pull/40" in comment
    assert "Trigger: PIT rule change: docs/PIT_DISCIPLINE.md" in comment
    assert "Allowed next action:" in comment
    assert linear.state_updates == [("issue-id", "safety-review-id")]


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


def test_dirty_pr_moves_to_rework_even_when_required_check_is_missing() -> None:
    issue = _issue("ARR-51")
    pr = _pr(
        56,
        title="ARR-51 Enforce durable backtest metadata replay constraints",
        merge_state_status="DIRTY",
        checks=(),
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


def test_choose_pr_ignores_identifier_mentions_in_proof_body() -> None:
    issue = _issue("ARR-41", state="Backlog")
    pull_requests = (
        _pr(
            41,
            title="ARR-47 Add admission steward for Objective-driven Todo admission",
            head_ref_name="codex/admission-steward",
            body=(
                "Live dry-run decisions:\n"
                "ARR-41 | promote | Backlog | approved Objective\n"
                "ARR-42 | promote | Backlog | approved Objective\n"
            ),
            state="MERGED",
            merged_at="2026-04-29T12:51:06Z",
        ),
    )

    pr = merge_steward.choose_pr_for_issue(issue, pull_requests)

    assert pr is None


def test_choose_pr_does_not_match_identifier_prefix() -> None:
    issue = _issue("ARR-4")
    pull_requests = (
        _pr(
            41,
            title="ARR-41 Add admission steward",
            head_ref_name="arr-41-admission-steward",
        ),
    )

    pr = merge_steward.choose_pr_for_issue(issue, pull_requests)

    assert pr is None


def test_parse_github_repo_handles_https_and_ssh_remotes() -> None:
    assert (
        merge_steward.parse_github_repo("https://github.com/SilverEnv/Silver.git")
        == "SilverEnv/Silver"
    )
    assert (
        merge_steward.parse_github_repo("git@github.com:SilverEnv/Silver.git")
        == "SilverEnv/Silver"
    )


def test_project_issue_reads_reuse_team_state_cache() -> None:
    linear = _RecordingLinearClient()

    linear.project_issues("silver")
    linear.project_issues("silver")

    assert sum("project(id:" in query for query in linear.queries) == 2
    assert sum("team(id:" in query for query in linear.queries) == 1


def test_merge_linear_rate_limit_error_uses_duration_ms() -> None:
    retry_after = merge_steward.linear_errors_retry_after_seconds(
        (
            {
                "message": "Only 2500 requests are allowed per 1 hour",
                "extensions": {"code": "RATELIMITED", "duration": 3600000},
            },
        )
    )

    assert retry_after == 3600


def test_merge_fast_watch_poll_interval_is_clamped() -> None:
    assert merge_steward.effective_watch_poll_interval(30) == 120


def _issue(
    identifier: str,
    *,
    state: str = "Merging",
    title: str = "Test issue",
    description: str = "",
):
    return merge_steward.LinearIssue(
        id="issue-id",
        identifier=identifier,
        title=title,
        url=f"https://linear.app/arrow1/issue/{identifier.lower()}/test",
        description=description,
        state=state,
        team_states={
            "Done": merge_steward.LinearState(id="done-id", name="Done"),
            "Rework": merge_steward.LinearState(id="rework-id", name="Rework"),
            "Safety Review": merge_steward.LinearState(
                id="safety-review-id",
                name="Safety Review",
            ),
        },
    )


def _changed_files(*paths: str):
    return tuple(
        merge_steward.ChangedFile(path=path, additions=1, deletions=0)
        for path in paths
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
    changed_files: tuple[object, ...] = (),
    diff: str | None = "",
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
        changed_files=changed_files,
        diff=diff,
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

    def with_diff(self, pr):
        return pr

    def queue_pull_request(self, number: int) -> None:
        self.queued.append(number)


class _RecordingLinearClient(merge_steward.LinearClient):
    def __init__(self) -> None:
        super().__init__("test-key")
        self.queries: list[str] = []

    def graphql(self, query, variables=None):
        self.queries.append(query)
        if "project(id:" in query:
            return {
                "project": {
                    "issues": {
                        "nodes": [
                            {
                                "id": "issue-41",
                                "identifier": "ARR-41",
                                "title": "First ticket",
                                "url": "https://linear.app/arrow1/issue/ARR-41",
                                "description": "Ticket description.",
                                "state": {"name": "Merging"},
                                "team": {"id": "team-1"},
                            },
                            {
                                "id": "issue-42",
                                "identifier": "ARR-42",
                                "title": "Second ticket",
                                "url": "https://linear.app/arrow1/issue/ARR-42",
                                "description": "Ticket description.",
                                "state": {"name": "Done"},
                                "team": {"id": "team-1"},
                            },
                        ],
                    },
                },
            }
        if "team(id:" in query:
            return {
                "team": {
                    "states": {
                        "nodes": [
                            {"id": "done-id", "name": "Done"},
                            {"id": "rework-id", "name": "Rework"},
                            {"id": "safety-id", "name": "Safety Review"},
                        ],
                    },
                },
            }
        raise AssertionError(f"unexpected query: {query}")
