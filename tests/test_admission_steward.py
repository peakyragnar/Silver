from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ADMISSION_STEWARD_SCRIPT = ROOT / "scripts" / "admission_steward.py"


def load_admission_steward_module():
    spec = importlib.util.spec_from_file_location(
        "admission_steward",
        ADMISSION_STEWARD_SCRIPT,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


admission_steward = load_admission_steward_module()


def test_promotes_approved_unblocked_backlog_ticket() -> None:
    issue = _issue("ARR-41", description=_description("wire-backtest-metadata-registry"))

    decision = admission_steward.decide_issue_admission(
        issue=issue,
        issues=(issue,),
        active_issues=(),
        open_pull_requests=(),
        approved_objectives={"wire-backtest-metadata-registry"},
    )

    assert decision.action == "promote"
    assert "approved Objective" in decision.reason


def test_skips_ticket_without_approved_objective() -> None:
    issue = _issue("ARR-41", description=_description("unknown-objective"))

    decision = admission_steward.decide_issue_admission(
        issue=issue,
        issues=(issue,),
        active_issues=(),
        open_pull_requests=(),
        approved_objectives={"wire-backtest-metadata-registry"},
    )

    assert decision.action == "skip"
    assert "not active/approved" in decision.reason


def test_waits_for_unfinished_blocker() -> None:
    blocker = _issue(
        "ARR-41",
        description=_description("wire-backtest-metadata-registry"),
        relations=(
            admission_steward.IssueRelation(
                type="blocks",
                related_identifier="ARR-42",
                related_state="Backlog",
            ),
        ),
    )
    blocked = _issue("ARR-42", description=_description("wire-backtest-metadata-registry"))

    decision = admission_steward.decide_issue_admission(
        issue=blocked,
        issues=(blocker, blocked),
        active_issues=(),
        open_pull_requests=(),
        approved_objectives={"wire-backtest-metadata-registry"},
    )

    assert decision.action == "wait"
    assert "ARR-41" in decision.reason


def test_done_blocker_allows_promotion() -> None:
    blocker = _issue(
        "ARR-41",
        state="Done",
        description=_description("wire-backtest-metadata-registry"),
        relations=(
            admission_steward.IssueRelation(
                type="blocks",
                related_identifier="ARR-42",
                related_state="Backlog",
            ),
        ),
    )
    blocked = _issue("ARR-42", description=_description("wire-backtest-metadata-registry"))

    decision = admission_steward.decide_issue_admission(
        issue=blocked,
        issues=(blocker, blocked),
        active_issues=(),
        open_pull_requests=(),
        approved_objectives={"wire-backtest-metadata-registry"},
    )

    assert decision.action == "promote"


def test_waits_when_open_pr_already_exists() -> None:
    issue = _issue("ARR-41", description=_description("wire-backtest-metadata-registry"))
    pr = admission_steward.PullRequest(
        title="ARR-41 Confirm metadata contract",
        head_ref_name="arr-41-contract",
        body="",
        state="OPEN",
    )

    decision = admission_steward.decide_issue_admission(
        issue=issue,
        issues=(issue,),
        active_issues=(),
        open_pull_requests=(pr,),
        approved_objectives={"wire-backtest-metadata-registry"},
    )

    assert decision.action == "wait"
    assert "open PR" in decision.reason


def test_capacity_limits_promotions() -> None:
    issues = (
        _issue("ARR-41", description=_description("wire-backtest-metadata-registry")),
        _issue("ARR-42", description=_description("wire-backtest-metadata-registry")),
        _issue("ARR-43", description=_description("wire-backtest-metadata-registry")),
    )

    decisions = admission_steward.select_admissions(
        issues=issues,
        open_pull_requests=(),
        approved_objectives={"wire-backtest-metadata-registry"},
        max_active=2,
        todo_buffer=2,
    )

    assert [decision.action for _, decision in decisions] == [
        "promote",
        "promote",
        "wait",
    ]
    assert "capacity full" in decisions[2][1].reason


def test_active_migration_lane_blocks_new_migration_owner() -> None:
    active = _issue(
        "ARR-50",
        state="In Progress",
        description=_description(
            "first-objective",
            owns=("- `db/migrations/`",),
        ),
    )
    candidate = _issue(
        "ARR-51",
        description=_description(
            "wire-backtest-metadata-registry",
            owns=("- `db/migrations/`",),
        ),
    )

    decision = admission_steward.decide_issue_admission(
        issue=candidate,
        issues=(active, candidate),
        active_issues=(active,),
        open_pull_requests=(),
        approved_objectives={"wire-backtest-metadata-registry"},
    )

    assert decision.action == "wait"
    assert "migration/schema lane" in decision.reason


def test_hard_exact_path_blocks_parallel_steward_edits() -> None:
    active = _issue(
        "ARR-50",
        state="In Progress",
        description=_description(
            "first-objective",
            owns=("- `scripts/merge_steward.py`",),
        ),
    )
    candidate = _issue(
        "ARR-51",
        description=_description(
            "wire-backtest-metadata-registry",
            owns=("- `scripts/merge_steward.py`",),
        ),
    )

    decision = admission_steward.decide_issue_admission(
        issue=candidate,
        issues=(active, candidate),
        active_issues=(active,),
        open_pull_requests=(),
        approved_objectives={"wire-backtest-metadata-registry"},
    )

    assert decision.action == "wait"
    assert "hard conflict" in decision.reason


def test_soft_same_file_conflict_can_promote() -> None:
    active = _issue(
        "ARR-50",
        state="In Progress",
        description=_description("first-objective", owns=("- `scripts/run_falsifier.py`",)),
    )
    candidate = _issue(
        "ARR-51",
        description=_description(
            "wire-backtest-metadata-registry",
            owns=("- `scripts/run_falsifier.py`",),
        ),
    )

    decision = admission_steward.decide_issue_admission(
        issue=candidate,
        issues=(active, candidate),
        active_issues=(active,),
        open_pull_requests=(),
        approved_objectives={"wire-backtest-metadata-registry"},
    )

    assert decision.action == "promote"


def test_apply_promotion_comments_and_moves_to_todo() -> None:
    issue = _issue("ARR-41", description=_description("wire-backtest-metadata-registry"))
    decision = admission_steward.AdmissionDecision(
        "promote",
        "approved Objective wire-backtest-metadata-registry; capacity available",
    )
    linear = _FakeLinear()

    admission_steward.apply_promotion(
        issue=issue,
        decision=decision,
        linear=linear,
    )

    assert len(linear.comments) == 1
    assert "## Admission Steward" in linear.comments[0][1]
    assert "wire-backtest-metadata-registry" in linear.comments[0][1]
    assert linear.state_updates == [("issue-id", "todo-id")]


def test_dry_run_reports_without_writes(capsys) -> None:
    issue = _issue("ARR-41", description=_description("wire-backtest-metadata-registry"))
    linear = _FakeLinear(issues=(issue,))
    github = _FakeGitHub()
    args = argparse.Namespace(
        project="silver",
        limit=100,
        max_active=5,
        todo_buffer=5,
        promote=False,
        dry_run=True,
    )

    admission_steward.run_once(args=args, linear=linear, github=github)

    output = capsys.readouterr().out
    assert "ARR-41 | promote" in output
    assert "DRY RUN" in output
    assert linear.comments == []
    assert linear.state_updates == []


def test_project_issues_reads_relations_without_issue_relation_calls() -> None:
    linear = _RecordingLinearClient()

    issues = linear.project_issues("silver")

    assert [issue.identifier for issue in issues] == ["ARR-41", "ARR-42"]
    assert issues[0].relations == (
        admission_steward.IssueRelation(
            type="blocks",
            related_identifier="ARR-42",
            related_state="Backlog",
        ),
    )
    assert sum("issue(id:" in query for query in linear.queries) == 0
    assert sum("team(id:" in query for query in linear.queries) == 1


def test_team_states_are_cached_between_project_reads() -> None:
    linear = _RecordingLinearClient()

    linear.project_issues("silver")
    linear.project_issues("silver")

    assert sum("project(id:" in query for query in linear.queries) == 2
    assert sum("team(id:" in query for query in linear.queries) == 1


def test_linear_rate_limit_error_uses_duration_ms() -> None:
    retry_after = admission_steward.linear_errors_retry_after_seconds(
        (
            {
                "message": "Only 2500 requests are allowed per 1 hour",
                "extensions": {"code": "RATELIMITED", "duration": 3600000},
            },
        )
    )

    assert retry_after == 3600


def test_fast_watch_poll_interval_is_clamped() -> None:
    assert admission_steward.effective_watch_poll_interval(30) == 120


def _issue(
    identifier: str,
    *,
    state: str = "Backlog",
    description: str = "",
    title: str = "Test issue",
    relations=(),
):
    return admission_steward.LinearIssue(
        id="issue-id",
        identifier=identifier,
        title=title,
        url=f"https://linear.app/arrow1/issue/{identifier.lower()}/test",
        description=description,
        state=state,
        team_states={
            "Todo": admission_steward.LinearState(id="todo-id", name="Todo"),
            "Done": admission_steward.LinearState(id="done-id", name="Done"),
        },
        relations=tuple(relations),
    )


def _description(
    objective: str,
    *,
    owns: tuple[str, ...] = ("- `docs/TESTING.md`",),
    conflict_zones: tuple[str, ...] = (),
) -> str:
    return "\n".join(
        (
            f"Parent Objective: {objective}",
            "",
            "Purpose:",
            "Test purpose.",
            "",
            "Owns:",
            *owns,
            "",
            "Conflict Zones:",
            *(conflict_zones or owns),
            "",
        )
    )


class _FakeLinear:
    def __init__(self, issues=()) -> None:
        self.issues = tuple(issues)
        self.comments: list[tuple[str, str]] = []
        self.state_updates: list[tuple[str, str]] = []

    def project_issues(self, project):
        return self.issues

    def create_comment(self, issue_id: str, body: str) -> None:
        self.comments.append((issue_id, body))

    def update_issue_state(self, issue_id: str, state_id: str) -> None:
        self.state_updates.append((issue_id, state_id))


class _FakeGitHub:
    def __init__(self, pull_requests=()) -> None:
        self.pull_requests = tuple(pull_requests)

    def list_open_pull_requests(self, limit: int):
        return self.pull_requests


class _RecordingLinearClient(admission_steward.LinearClient):
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
                                "description": _description(
                                    "wire-backtest-metadata-registry"
                                ),
                                "state": {"name": "Backlog"},
                                "team": {"id": "team-1"},
                                "relations": {
                                    "nodes": [
                                        {
                                            "type": "blocks",
                                            "relatedIssue": {
                                                "identifier": "ARR-42",
                                                "state": {"name": "Backlog"},
                                            },
                                        },
                                    ],
                                },
                            },
                            {
                                "id": "issue-42",
                                "identifier": "ARR-42",
                                "title": "Second ticket",
                                "url": "https://linear.app/arrow1/issue/ARR-42",
                                "description": _description(
                                    "wire-backtest-metadata-registry"
                                ),
                                "state": {"name": "Backlog"},
                                "team": {"id": "team-1"},
                                "relations": {"nodes": []},
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
                            {"id": "todo-id", "name": "Todo"},
                            {"id": "done-id", "name": "Done"},
                        ],
                    },
                },
            }
        raise AssertionError(f"unexpected query: {query}")
