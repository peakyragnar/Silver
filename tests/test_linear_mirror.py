from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LINEAR_MIRROR_SCRIPT = ROOT / "scripts" / "linear_mirror.py"


def load_linear_mirror_module():
    spec = importlib.util.spec_from_file_location(
        "linear_mirror",
        LINEAR_MIRROR_SCRIPT,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


linear_mirror = load_linear_mirror_module()
work_ledger = linear_mirror.work_ledger


def test_dry_run_plans_create_actions_without_writes(tmp_path: Path) -> None:
    ledger_path = _seed_ledger(tmp_path)
    client = _FakeLinearClient()

    with work_ledger.connect_existing(ledger_path) as connection:
        actions = linear_mirror.mirror_once(
            connection=connection,
            client=client,
            project="silver",
            team_id="team-1",
            limit=100,
            apply=False,
        )
        rows = connection.execute(
            "SELECT linear_identifier FROM tickets ORDER BY sequence"
        ).fetchall()

    assert [(action.action, action.target_state) for action in actions] == [
        ("create", "Todo"),
        ("create", "Backlog"),
    ]
    assert client.created == []
    assert [row["linear_identifier"] for row in rows] == [None, None]


def test_apply_creates_linear_issues_and_records_mirror_state(
    tmp_path: Path,
) -> None:
    ledger_path = _seed_ledger(tmp_path)
    client = _FakeLinearClient()

    with work_ledger.connect_existing(ledger_path) as connection:
        actions = linear_mirror.mirror_once(
            connection=connection,
            client=client,
            project="silver",
            team_id="team-1",
            limit=100,
            apply=True,
        )
        tickets = connection.execute(
            "SELECT id, linear_identifier FROM tickets ORDER BY sequence"
        ).fetchall()
        mirror_rows = connection.execute(
            "SELECT ticket_id, linear_state FROM linear_mirror_state ORDER BY ticket_id"
        ).fetchall()

    assert [action.action for action in actions] == ["create", "create"]
    assert [item["linear_identifier"] for item in tickets] == ["ARR-1", "ARR-2"]
    assert [(item["ticket_id"], item["linear_state"]) for item in mirror_rows] == [
        ("local-runtime-ledger-001", "Todo"),
        ("local-runtime-ledger-002", "Backlog"),
    ]
    assert len(client.created) == 2
    description = client.created[0]["description"]
    assert "Ledger Ticket: local-runtime-ledger-001" in description
    assert "Ticket Role:" in description
    assert "Dependency Group:" in description
    assert "Contracts Touched:" in description
    assert "Risk Class:" in description
    assert "PR URL:" in description
    assert "Branch:" in description
    assert "Objective Impact:" in description


def test_existing_issue_state_updates_from_ledger_status(tmp_path: Path) -> None:
    ledger_path = _seed_ledger(tmp_path)
    current_description = _linear_description(
        ledger_path,
        "local-runtime-ledger-001",
    )
    client = _FakeLinearClient(
        issues=(
            linear_mirror.LinearIssue(
                id="issue-1",
                identifier="ARR-1",
                title="Existing",
                description=current_description,
                state="Backlog",
                team_id="team-1",
            ),
        )
    )

    with work_ledger.connect_existing(ledger_path) as connection:
        actions = linear_mirror.mirror_once(
            connection=connection,
            client=client,
            project="silver",
            team_id=None,
            limit=100,
            apply=True,
        )
        mirror_state = connection.execute(
            """
            SELECT linear_state FROM linear_mirror_state
            WHERE ticket_id = 'local-runtime-ledger-001'
            """
        ).fetchone()["linear_state"]

    assert actions[0].action == "update_state"
    assert client.updated == [("issue-1", "todo-id")]
    assert mirror_state == "Todo"


def test_current_existing_issue_is_noop(tmp_path: Path) -> None:
    ledger_path = _seed_ledger(tmp_path)
    current_description = _linear_description(
        ledger_path,
        "local-runtime-ledger-001",
    )
    client = _FakeLinearClient(
        issues=(
            linear_mirror.LinearIssue(
                id="issue-1",
                identifier="ARR-1",
                title="Existing",
                description=current_description,
                state="Todo",
                team_id="team-1",
            ),
        )
    )

    with work_ledger.connect_existing(ledger_path) as connection:
        actions = linear_mirror.mirror_once(
            connection=connection,
            client=client,
            project="silver",
            team_id=None,
            limit=100,
            apply=False,
        )

    assert actions[0].action == "noop"
    assert actions[0].linear_issue is not None
    assert actions[0].linear_issue.identifier == "ARR-1"


def test_existing_issue_description_updates_when_metadata_is_missing(
    tmp_path: Path,
) -> None:
    ledger_path = _seed_ledger(tmp_path)
    client = _FakeLinearClient(
        issues=(
            linear_mirror.LinearIssue(
                id="issue-1",
                identifier="ARR-1",
                title="Existing",
                description="Ledger Ticket: local-runtime-ledger-001\n",
                state="Todo",
                team_id="team-1",
            ),
        )
    )

    with work_ledger.connect_existing(ledger_path) as connection:
        actions = linear_mirror.mirror_once(
            connection=connection,
            client=client,
            project="silver",
            team_id=None,
            limit=100,
            apply=True,
        )

    assert actions[0].action == "update_description"
    assert client.updated_descriptions
    issue_id, description = client.updated_descriptions[0]
    assert issue_id == "issue-1"
    assert "Ticket Role:" in description
    assert "Contracts Touched:" in description


def test_description_compare_accepts_linear_markdown_readback() -> None:
    expected = "\n".join(
        [
            "Owns:",
            "- `tests/`",
            "- `scripts/`",
            "",
            "Validation:",
            "- `python -m pytest`",
            "",
            "Dependencies:",
            "- `Wire runners to create `model_runs``",
        ]
    )
    linear_readback = "\n".join(
        [
            "Owns:",
            "",
            "* `tests/`",
            "* `scripts/`",
            "",
            "Validation:",
            "",
            "* `python -m pytest`",
            "",
            "Dependencies:",
            "",
            "* `Wire runners to create `model_runs\\`\\``",
        ]
    )

    assert linear_mirror.descriptions_match(linear_readback, expected)


def test_description_includes_pr_evidence_and_latest_steward_event(
    tmp_path: Path,
) -> None:
    ledger_path = _seed_ledger(tmp_path)
    with work_ledger.connect_existing(ledger_path) as connection:
        now = work_ledger.utc_now()
        connection.execute(
            """
            UPDATE tickets
            SET status = 'Rework',
                branch = 'arr-55-traceability',
                pr_url = 'https://github.com/SilverEnv/Silver/pull/60',
                updated_at = ?
            WHERE id = 'local-runtime-ledger-001'
            """,
            (now,),
        )
        work_ledger.insert_event(
            connection,
            ticket_id="local-runtime-ledger-001",
            objective_id="local-runtime-ledger",
            event_type="integration_repair_requested",
            from_status="Rework",
            to_status="Rework",
            message="Repair PR #60: resolve merge conflicts and rerun tests.",
            actor="integration_steward",
            created_at=now,
        )
        ticket = {
            item.id: item for item in linear_mirror.mirror_tickets(connection)
        }["local-runtime-ledger-001"]

    description = linear_mirror.linear_description(ticket)

    assert "PR URL: https://github.com/SilverEnv/Silver/pull/60" in description
    assert "Branch: arr-55-traceability" in description
    assert "Latest Steward Event:" in description
    assert "resolve merge conflicts" in description


def test_check_mode_avoids_network_and_ledger_reads(tmp_path: Path, capsys) -> None:
    ledger_path = tmp_path / "missing.db"

    exit_code = linear_mirror.main(
        [
            "--ledger",
            str(ledger_path),
            "--project",
            "silver",
            "--check",
        ]
    )

    assert exit_code == 0
    assert "configuration is valid" in capsys.readouterr().out


def _seed_ledger(tmp_path: Path) -> Path:
    root = _write_repo_with_objective(tmp_path / "repo")
    ledger_path = tmp_path / "ledger.db"
    work_ledger.initialize_ledger(ledger_path)
    with work_ledger.connect_existing(ledger_path) as connection:
        work_ledger.import_objectives(
            connection,
            work_ledger.active_objective_proposals(root),
        )
        work_ledger.transition_ticket(
            connection,
            ticket_id="local-runtime-ledger-001",
            status="Ready",
            actor="test",
            message="ready for mirror",
        )
    return ledger_path


def _linear_description(ledger_path: Path, ticket_id: str) -> str:
    with work_ledger.connect_existing(ledger_path) as connection:
        tickets = {
            ticket.id: ticket
            for ticket in linear_mirror.mirror_tickets(connection)
        }
    return linear_mirror.linear_description(tickets[ticket_id])


def _write_repo_with_objective(root: Path) -> Path:
    active_dir = root / "docs" / "objectives" / "active"
    completed_dir = root / "docs" / "objectives" / "completed"
    active_dir.mkdir(parents=True)
    completed_dir.mkdir(parents=True)
    (root / "docs" / "Symphony-Operation.md").write_text(
        "# Operation\n\nPlanning Steward\nObjective Impact\n",
        encoding="utf-8",
    )
    (root / "WORKFLOW.md").write_text(
        "## Proof Packet\n\n- Objective Impact\n",
        encoding="utf-8",
    )
    (root / "docs" / "exec-plans" / "active").mkdir(parents=True)
    (root / "db" / "migrations").mkdir(parents=True)
    (root / "src").mkdir()
    (root / "scripts").mkdir()
    (root / "scripts" / "planning_steward.py").write_text("# test\n")
    (active_dir / "local-runtime-ledger.md").write_text(
        "\n".join(
            [
                "# local-runtime-ledger",
                "",
                "Objective:",
                "Mirror local ledger tickets into Linear.",
                "",
                "User Value:",
                "Michael gets fast local state while Symphony can still see tickets.",
                "",
                "Why Now:",
                "The local ledger needs a bridge to the current Linear workflow.",
                "",
                "Done When:",
                "- Ledger tickets can produce Linear mirror actions.",
                "",
                "Out Of Scope:",
                "- No worker dispatcher replacement.",
                "",
                "Guardrails:",
                "- Default to dry-run.",
                "",
                "Expected Tickets:",
                "- Mirror Ready ticket",
                "  Purpose: Create or update the visible Linear issue.",
                "  Objective Impact: A local Ready ticket becomes visible to Symphony as Todo.",
                "  Technical Summary: Map local ledger state into Linear state.",
                "  Owns:",
                "  - scripts/linear_mirror.py",
                "  Conflict Zones:",
                "  - scripts/linear_mirror.py",
                "  Validation:",
                "  - python -m pytest tests/test_linear_mirror.py",
                "- Keep waiting ticket",
                "  Purpose: Keep Backlog local state visible but inactive.",
                "  Objective Impact: A local Backlog ticket stays planned without starting agents.",
                "  Technical Summary: Map Backlog to Linear Backlog.",
                "  Owns:",
                "  - scripts/linear_mirror.py",
                "  Conflict Zones:",
                "  - scripts/linear_mirror.py",
                "  Validation:",
                "  - python -m pytest tests/test_linear_mirror.py",
                "",
                "Validation:",
                "- python -m pytest tests/test_linear_mirror.py",
                "",
                "Conflict Zones:",
                "- scripts/linear_mirror.py",
            ]
        ),
        encoding="utf-8",
    )
    return root


class _FakeLinearClient:
    def __init__(self, issues=()) -> None:
        self.issues = list(issues)
        self.created: list[dict[str, str]] = []
        self.updated: list[tuple[str, str]] = []
        self.updated_descriptions: list[tuple[str, str]] = []

    def project_snapshot(self, project: str, *, limit: int):
        assert project == "silver"
        assert limit == 100
        return linear_mirror.LinearProject(
            id="project-id",
            issues=tuple(self.issues),
        )

    def team_states(self, team_id: str):
        assert team_id == "team-1"
        return {
            "Backlog": linear_mirror.LinearState(id="backlog-id", name="Backlog"),
            "Todo": linear_mirror.LinearState(id="todo-id", name="Todo"),
            "In Progress": linear_mirror.LinearState(
                id="progress-id",
                name="In Progress",
            ),
            "Rework": linear_mirror.LinearState(id="rework-id", name="Rework"),
            "Merging": linear_mirror.LinearState(id="merging-id", name="Merging"),
            "Done": linear_mirror.LinearState(id="done-id", name="Done"),
            "Safety Review": linear_mirror.LinearState(
                id="safety-id",
                name="Safety Review",
            ),
            "Canceled": linear_mirror.LinearState(id="canceled-id", name="Canceled"),
            "Duplicate": linear_mirror.LinearState(
                id="duplicate-id",
                name="Duplicate",
            ),
        }

    def create_issue(
        self,
        *,
        team_id: str,
        project_id: str,
        state_id: str,
        title: str,
        description: str,
    ):
        assert team_id == "team-1"
        assert project_id == "project-id"
        state_by_id = {
            "backlog-id": "Backlog",
            "todo-id": "Todo",
        }
        identifier = f"ARR-{len(self.created) + 1}"
        issue = linear_mirror.LinearIssue(
            id=f"issue-{len(self.created) + 1}",
            identifier=identifier,
            title=title,
            description=description,
            state=state_by_id[state_id],
            team_id=team_id,
        )
        self.created.append(
            {
                "identifier": identifier,
                "state_id": state_id,
                "title": title,
                "description": description,
            }
        )
        self.issues.append(issue)
        return issue

    def update_issue_state(self, issue_id: str, state_id: str) -> None:
        self.updated.append((issue_id, state_id))

    def update_issue_description(self, issue_id: str, description: str) -> None:
        self.updated_descriptions.append((issue_id, description))
