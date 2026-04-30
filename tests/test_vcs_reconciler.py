from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
VCS_RECONCILER_SCRIPT = ROOT / "scripts" / "vcs_reconciler.py"


def load_vcs_reconciler_module():
    spec = importlib.util.spec_from_file_location(
        "vcs_reconciler",
        VCS_RECONCILER_SCRIPT,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


vcs_reconciler = load_vcs_reconciler_module()
work_ledger = vcs_reconciler.work_ledger
merge_steward = vcs_reconciler.merge_steward


def test_merged_pr_marks_ticket_done_and_records_pr_evidence(tmp_path: Path) -> None:
    connection = _ledger_connection(tmp_path)
    _insert_ticket(
        connection,
        ticket_id="portable-orchestration-core-001",
        status="In Progress",
        linear_identifier="ARR-55",
    )
    pr = _pr(
        60,
        title="ARR-55 Prove falsifier traceability model join",
        state="MERGED",
        merged_at="2026-04-30T08:00:00Z",
        body="Ledger Ticket: `portable-orchestration-core-001`",
        head_ref_name="arr-55-traceability",
    )

    actions = vcs_reconciler.reconcile_prs(
        connection,
        (pr,),
        required_checks=("Python 3.10 checks",),
        apply=True,
    )

    row = connection.execute(
        "SELECT status, branch, pr_url FROM tickets WHERE id = ?",
        ("portable-orchestration-core-001",),
    ).fetchone()
    events = work_ledger.recent_events(
        connection,
        ticket_id="portable-orchestration-core-001",
        limit=2,
    )

    assert [action.action for action in actions] == ["mark_done"]
    assert row["status"] == "Done"
    assert row["branch"] == "arr-55-traceability"
    assert row["pr_url"] == "https://github.com/SilverEnv/Silver/pull/60"
    assert {event["event_type"] for event in events} == {
        "vcs_pr_observed",
        "status_transition",
    }


def test_dry_run_does_not_write_ledger_state(tmp_path: Path) -> None:
    connection = _ledger_connection(tmp_path)
    _insert_ticket(
        connection,
        ticket_id="portable-orchestration-core-001",
        status="In Progress",
        linear_identifier="ARR-55",
    )

    actions = vcs_reconciler.reconcile_prs(
        connection,
        (_pr(60, title="ARR-55 Done", state="MERGED", merged_at="now"),),
        required_checks=("Python 3.10 checks",),
        apply=False,
    )

    row = connection.execute(
        "SELECT status, branch, pr_url FROM tickets WHERE id = ?",
        ("portable-orchestration-core-001",),
    ).fetchone()

    assert [action.action for action in actions] == ["mark_done"]
    assert row["status"] == "In Progress"
    assert row["branch"] is None
    assert row["pr_url"] is None


def test_green_open_pr_moves_ticket_to_merging(tmp_path: Path) -> None:
    connection = _ledger_connection(tmp_path)
    _insert_ticket(
        connection,
        ticket_id="portable-orchestration-core-001",
        status="In Progress",
        linear_identifier="ARR-55",
    )

    actions = vcs_reconciler.reconcile_prs(
        connection,
        (_pr(61, title="ARR-55 Add VCS reconciliation", merge_state_status="CLEAN"),),
        required_checks=("Python 3.10 checks",),
        apply=True,
    )

    ticket = work_ledger.select_ticket(connection, "portable-orchestration-core-001")

    assert [action.action for action in actions] == ["move_merging"]
    assert ticket.status == "Merging"


def test_dirty_open_pr_moves_ticket_to_rework(tmp_path: Path) -> None:
    connection = _ledger_connection(tmp_path)
    _insert_ticket(
        connection,
        ticket_id="portable-orchestration-core-001",
        status="Merging",
        linear_identifier="ARR-55",
    )

    actions = vcs_reconciler.reconcile_prs(
        connection,
        (_pr(61, title="ARR-55 Add VCS reconciliation", merge_state_status="DIRTY"),),
        required_checks=("Python 3.10 checks",),
        apply=True,
    )

    ticket = work_ledger.select_ticket(connection, "portable-orchestration-core-001")

    assert [action.action for action in actions] == ["move_rework"]
    assert "merge conflicts" in actions[0].reason
    assert ticket.status == "Rework"


def test_blocked_ticket_is_not_unblocked_by_green_pr(tmp_path: Path) -> None:
    connection = _ledger_connection(tmp_path)
    _insert_ticket(
        connection,
        ticket_id="portable-orchestration-core-001",
        status="Blocked",
        linear_identifier="ARR-55",
    )

    actions = vcs_reconciler.reconcile_prs(
        connection,
        (_pr(61, title="ARR-55 Add VCS reconciliation", merge_state_status="CLEAN"),),
        required_checks=("Python 3.10 checks",),
        apply=True,
    )

    ticket = work_ledger.select_ticket(connection, "portable-orchestration-core-001")

    assert [action.action for action in actions] == ["wait"]
    assert ticket.status == "Blocked"


def test_do_not_touch_scope_drift_moves_ticket_to_safety_review(
    tmp_path: Path,
) -> None:
    connection = _ledger_connection(tmp_path)
    _insert_ticket(
        connection,
        ticket_id="portable-orchestration-core-001",
        status="Merging",
        linear_identifier="ARR-55",
        do_not_touch=("scripts/planning_steward.py",),
    )

    actions = vcs_reconciler.reconcile_prs(
        connection,
        (
            _pr(
                61,
                title="ARR-55 Change planning steward",
                changed_files=_changed_files("scripts/planning_steward.py"),
                diff="+active_states: [Todo, In Progress, Rework]\n",
            ),
        ),
        required_checks=("Python 3.10 checks",),
        apply=True,
    )

    ticket = work_ledger.select_ticket(connection, "portable-orchestration-core-001")

    assert [action.action for action in actions] == ["move_safety_review"]
    assert "scope drift" in actions[0].reason
    assert ticket.status == "Safety Review"


def test_planned_contract_docs_only_pit_change_can_move_to_merging(
    tmp_path: Path,
) -> None:
    connection = _ledger_connection(tmp_path)
    _insert_ticket(
        connection,
        ticket_id="portable-orchestration-core-001",
        status="Safety Review",
        linear_identifier="ARR-55",
        ticket_role="contract",
        owns=("docs/PIT_DISCIPLINE.md", "SPEC.md"),
    )

    actions = vcs_reconciler.reconcile_prs(
        connection,
        (
            _pr(
                61,
                title="ARR-55 Define PIT evidence contract",
                changed_files=_changed_files("docs/PIT_DISCIPLINE.md", "SPEC.md"),
                diff=(
                    "diff --git a/docs/PIT_DISCIPLINE.md b/docs/PIT_DISCIPLINE.md\n"
                    "+++ b/docs/PIT_DISCIPLINE.md\n"
                    "+Falsifier evidence must use feature values visible at each asof_date.\n"
                    "diff --git a/SPEC.md b/SPEC.md\n"
                    "+++ b/SPEC.md\n"
                    "+Reports must expose available-at policy versions.\n"
                ),
            ),
        ),
        required_checks=("Python 3.10 checks",),
        apply=True,
    )

    ticket = work_ledger.select_ticket(connection, "portable-orchestration-core-001")

    assert [action.action for action in actions] == ["move_merging"]
    assert "planned contract docs-only PIT clarification" in actions[0].reason
    assert ticket.status == "Merging"


def test_planned_contract_docs_only_security_hardening_can_move_to_merging(
    tmp_path: Path,
) -> None:
    connection = _ledger_connection(tmp_path)
    _insert_ticket(
        connection,
        ticket_id="raw-vault-failed-fmp-responses-001",
        status="Safety Review",
        linear_identifier="ARR-62",
        ticket_role="contract",
        owns=("docs/SECURITY.md", "docs/ARCHITECTURE.md"),
        do_not_touch=(".env", "src/silver/sources/fmp/client.py"),
    )

    actions = vcs_reconciler.reconcile_prs(
        connection,
        (
            _pr(
                76,
                title="ARR-62 Define failed FMP raw-vault audit contract",
                changed_files=_changed_files(
                    "docs/SECURITY.md",
                    "docs/ARCHITECTURE.md",
                ),
                diff=(
                    "diff --git a/docs/SECURITY.md b/docs/SECURITY.md\n"
                    "+++ b/docs/SECURITY.md\n"
                    "+FMP raw-vault request metadata must redact API keys before persistence.\n"
                    "diff --git a/docs/ARCHITECTURE.md b/docs/ARCHITECTURE.md\n"
                    "+++ b/docs/ARCHITECTURE.md\n"
                    "+Failed vendor responses are persisted before parsing or raising.\n"
                ),
            ),
        ),
        required_checks=("Python 3.10 checks",),
        apply=True,
    )

    ticket = work_ledger.select_ticket(
        connection,
        "raw-vault-failed-fmp-responses-001",
    )

    assert [action.action for action in actions] == ["move_merging"]
    assert "planned contract docs-only security hardening" in actions[0].reason
    assert ticket.status == "Merging"


def test_pit_doc_change_with_deletions_still_requires_safety_review(
    tmp_path: Path,
) -> None:
    connection = _ledger_connection(tmp_path)
    _insert_ticket(
        connection,
        ticket_id="portable-orchestration-core-001",
        status="Merging",
        linear_identifier="ARR-55",
        ticket_role="contract",
        owns=("docs/PIT_DISCIPLINE.md",),
    )

    actions = vcs_reconciler.reconcile_prs(
        connection,
        (
            _pr(
                61,
                title="ARR-55 Rewrite PIT evidence contract",
                changed_files=(
                    merge_steward.ChangedFile(
                        path="docs/PIT_DISCIPLINE.md",
                        additions=1,
                        deletions=1,
                    ),
                ),
                diff="+available_at is now optional for review-only reports\n",
            ),
        ),
        required_checks=("Python 3.10 checks",),
        apply=True,
    )

    ticket = work_ledger.select_ticket(connection, "portable-orchestration-core-001")

    assert [action.action for action in actions] == ["move_safety_review"]
    assert "PIT rule change" in actions[0].reason
    assert ticket.status == "Safety Review"


def test_rework_ticket_waits_for_repair_before_reclassification(
    tmp_path: Path,
) -> None:
    connection = _ledger_connection(tmp_path)
    _insert_ticket(
        connection,
        ticket_id="portable-orchestration-core-001",
        status="Rework",
        linear_identifier="ARR-55",
        ticket_role="implementation",
        risk_class="semantic",
        owns=("scripts/run_falsifier.py",),
    )

    actions = vcs_reconciler.reconcile_prs(
        connection,
        (
            _pr(
                61,
                title="ARR-55 Await repair",
                changed_files=_changed_files("scripts/run_falsifier.py"),
                diff="+metrics[\"sharpe\"] = gross_return.mean()\n",
            ),
        ),
        required_checks=("Python 3.10 checks",),
        apply=True,
    )

    ticket = work_ledger.select_ticket(connection, "portable-orchestration-core-001")

    assert [action.action for action in actions] == ["wait"]
    assert actions[0].reason == "ticket is already in Rework"
    assert ticket.status == "Rework"


def test_planned_semantic_owned_dirty_pr_moves_to_rework(
    tmp_path: Path,
) -> None:
    connection = _ledger_connection(tmp_path)
    _insert_ticket(
        connection,
        ticket_id="portable-orchestration-core-001",
        status="Merging",
        linear_identifier="ARR-55",
        ticket_role="implementation",
        risk_class="semantic",
        owns=("scripts/run_falsifier.py", "tests/test_run_falsifier_cli.py"),
    )

    actions = vcs_reconciler.reconcile_prs(
        connection,
        (
            _pr(
                61,
                title="ARR-55 Stabilize run identity",
                merge_state_status="DIRTY",
                changed_files=_changed_files(
                    "scripts/run_falsifier.py",
                    "tests/test_run_falsifier_cli.py",
                ),
                diff=(
                    "+model_run_key = stable_digest(identity_payload)\n"
                    "+metrics[\"model_run_key\"] = model_run_key\n"
                ),
            ),
        ),
        required_checks=("Python 3.10 checks",),
        apply=True,
    )

    ticket = work_ledger.select_ticket(connection, "portable-orchestration-core-001")

    assert [action.action for action in actions] == ["move_rework"]
    assert "planned semantic change in ticket-owned paths" in actions[0].reason
    assert "merge conflicts" in actions[0].reason
    assert ticket.status == "Rework"


def test_linear_identifier_matching_ignores_body_mentions_and_prefixes(
    tmp_path: Path,
) -> None:
    connection = _ledger_connection(tmp_path)
    _insert_ticket(
        connection,
        ticket_id="portable-orchestration-core-001",
        sequence=1,
        linear_identifier="ARR-4",
    )
    _insert_ticket(
        connection,
        ticket_id="portable-orchestration-core-002",
        sequence=2,
        linear_identifier="ARR-41",
    )

    actions = vcs_reconciler.reconcile_prs(
        connection,
        (
            _pr(
                41,
                title="ARR-41 Add admission steward",
                body="Proof mentions ARR-4 as a previous example.",
                head_ref_name="arr-41-admission-steward",
            ),
        ),
        required_checks=("Python 3.10 checks",),
        apply=False,
    )

    assert [action.ticket_id for action in actions] == [
        "portable-orchestration-core-002",
    ]


def _ledger_connection(tmp_path: Path):
    ledger_path = tmp_path / "ledger.db"
    work_ledger.initialize_ledger(ledger_path)
    return work_ledger.connect_existing(ledger_path)


def _insert_ticket(
    connection,
    *,
    ticket_id: str,
    sequence: int = 1,
    status: str = "In Progress",
    linear_identifier: str | None = None,
    ticket_role: str = "integration",
    risk_class: str = "low",
    owns: tuple[str, ...] = ("scripts/vcs_reconciler.py",),
    do_not_touch: tuple[str, ...] = (),
) -> None:
    now = work_ledger.utc_now()
    objective_id = "portable-orchestration-core"
    with connection:
        connection.execute(
            """
            INSERT OR IGNORE INTO objectives (
              id, title, user_value, why_now, source_path, status,
              validation_json, conflict_zones_json, imported_at, updated_at
            )
            VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, ?)
            """,
            (
                objective_id,
                "Build the portable objective-to-ticket orchestration core",
                "Michael can run objective-aware agents.",
                "Ticket-by-ticket execution needs PR readback.",
                work_ledger.OBJECTIVE_STATUS_ACTIVE,
                work_ledger.dumps_json(("git diff --check",)),
                work_ledger.dumps_json(("scripts/",)),
                now,
                now,
            ),
        )
        connection.execute(
            """
            INSERT INTO tickets (
              id, objective_id, sequence, title, purpose, objective_impact,
              technical_summary, ticket_role, dependency_group,
              contracts_touched_json, status, risk_class, conflict_domain,
              owns_json, do_not_touch_json, dependencies_json,
              conflict_zones_json, validation_json, proof_packet_json,
              branch, pr_url, linear_identifier, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    NULL, NULL, ?, ?, ?)
            """,
            (
                ticket_id,
                objective_id,
                sequence,
                "Add VCS reconciliation",
                "Read PR state and update the local ledger.",
                "The build system knows what landed after Symphony finishes.",
                "Classify GitHub PRs in Objective context.",
                ticket_role,
                "orchestration-core",
                work_ledger.dumps_json(("objective-dag",)),
                status,
                risk_class,
                "orchestration",
                work_ledger.dumps_json(owns),
                work_ledger.dumps_json(do_not_touch),
                work_ledger.dumps_json(()),
                work_ledger.dumps_json(("scripts/",)),
                work_ledger.dumps_json(("python -m pytest",)),
                work_ledger.dumps_json(("PR link", "ledger state change")),
                linear_identifier,
                now,
                now,
            ),
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
