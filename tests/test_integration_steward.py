from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INTEGRATION_STEWARD_SCRIPT = ROOT / "scripts" / "integration_steward.py"


def load_integration_steward_module():
    spec = importlib.util.spec_from_file_location(
        "integration_steward",
        INTEGRATION_STEWARD_SCRIPT,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


integration_steward = load_integration_steward_module()
work_ledger = integration_steward.work_ledger


def test_rework_ticket_gets_merge_conflict_repair_packet(tmp_path: Path) -> None:
    connection = _ledger_connection(tmp_path)
    _insert_rework_ticket(
        connection,
        blocker="PR #61 https://github.com/SilverEnv/Silver/pull/61: PR has merge conflicts",
    )

    packets = integration_steward.repair_packets(connection)

    assert len(packets) == 1
    packet = packets[0]
    assert packet.repair_kind == "merge_conflict"
    assert packet.pr_url == "https://github.com/SilverEnv/Silver/pull/61"
    assert packet.branch == "arr-61-repair"
    assert "Allowed Scope:" in packet.body
    assert "`scripts/vcs_reconciler.py`" in packet.body
    assert "Do Not Touch:" in packet.body
    assert "Safety Review" in packet.body
    assert "move the ticket back to Merging" in packet.body


def test_failed_check_repair_packet_points_worker_at_check_output(
    tmp_path: Path,
) -> None:
    connection = _ledger_connection(tmp_path)
    _insert_rework_ticket(
        connection,
        blocker="PR #61 https://github.com/SilverEnv/Silver/pull/61: failed required check(s): Python 3.10 checks=FAILURE",
    )

    packet = integration_steward.repair_packets(connection)[0]

    assert packet.repair_kind == "failed_check"
    assert "inspect the failing check output" in packet.body


def test_missing_pr_evidence_packet_does_not_guess_branch(tmp_path: Path) -> None:
    connection = _ledger_connection(tmp_path)
    _insert_rework_ticket(
        connection,
        blocker="Rework requested by operator without PR evidence.",
        branch=None,
        pr_url=None,
    )

    packet = integration_steward.repair_packets(connection)[0]

    assert packet.repair_kind == "missing_pr_evidence"
    assert "Find or attach the matching PR" in packet.body
    assert "Do not guess the branch" in packet.body


def test_apply_records_repair_packet_once(tmp_path: Path) -> None:
    connection = _ledger_connection(tmp_path)
    _insert_rework_ticket(
        connection,
        blocker="PR #61 https://github.com/SilverEnv/Silver/pull/61: PR has merge conflicts",
    )
    packets = integration_steward.repair_packets(connection)

    integration_steward.record_repair_packets(connection, packets)
    integration_steward.record_repair_packets(connection, packets)

    count = connection.execute(
        """
        SELECT COUNT(*) FROM ticket_events
        WHERE ticket_id = 'portable-orchestration-core-001'
          AND event_type = 'integration_repair_requested'
          AND actor = 'integration_steward'
        """
    ).fetchone()[0]
    latest = work_ledger.recent_events(
        connection,
        ticket_id="portable-orchestration-core-001",
        limit=1,
    )[0]

    assert count == 1
    assert latest["event_type"] == "integration_repair_requested"
    assert "Integration Repair Packet" in latest["message"]


def test_cli_check_uses_existing_ledger_without_writes(
    tmp_path: Path,
    capsys,
) -> None:
    ledger_path = tmp_path / "ledger.db"
    work_ledger.initialize_ledger(ledger_path)

    exit_code = integration_steward.main(
        ["--ledger", str(ledger_path), "--check"],
    )

    assert exit_code == 0
    assert "configuration is valid" in capsys.readouterr().out


def _ledger_connection(tmp_path: Path):
    ledger_path = tmp_path / "ledger.db"
    work_ledger.initialize_ledger(ledger_path)
    return work_ledger.connect_existing(ledger_path)


def _insert_rework_ticket(
    connection,
    *,
    blocker: str,
    branch: str | None = "arr-61-repair",
    pr_url: str | None = "https://github.com/SilverEnv/Silver/pull/61",
) -> None:
    now = work_ledger.utc_now()
    objective_id = "portable-orchestration-core"
    ticket_id = "portable-orchestration-core-001"
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
                "Michael can run objective-aware repair lanes.",
                "Rework needs precise repair packets.",
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
            VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?, 'Rework', ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ticket_id,
                objective_id,
                "Add integration repair packets",
                "Make Rework actionable for Symphony workers.",
                "Routine repair can be picked up without Michael restating context.",
                "Generate a bounded repair packet from ledger and PR evidence.",
                "integration",
                "orchestration-core",
                work_ledger.dumps_json(("objective-dag",)),
                "low",
                "orchestration",
                work_ledger.dumps_json(("scripts/vcs_reconciler.py",)),
                work_ledger.dumps_json(("db/migrations/",)),
                work_ledger.dumps_json(()),
                work_ledger.dumps_json(("scripts/",)),
                work_ledger.dumps_json(("python -m pytest", "ruff check .")),
                work_ledger.dumps_json(("PR link", "validation output")),
                branch,
                pr_url,
                "ARR-61",
                now,
                now,
            ),
        )
        work_ledger.insert_event(
            connection,
            ticket_id=ticket_id,
            objective_id=objective_id,
            event_type="status_transition",
            from_status="Merging",
            to_status="Rework",
            message=blocker,
            actor="vcs_reconciler",
            created_at=now,
        )
