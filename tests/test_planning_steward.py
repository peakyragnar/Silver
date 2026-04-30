from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
PLANNING_STEWARD_SCRIPT = ROOT / "scripts" / "planning_steward.py"


def load_planning_steward_module():
    spec = importlib.util.spec_from_file_location(
        "planning_steward",
        PLANNING_STEWARD_SCRIPT,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


planning_steward = load_planning_steward_module()


def test_collect_context_detects_repo_signals(tmp_path: Path) -> None:
    _write_minimal_repo(tmp_path)

    context = planning_steward.collect_context(tmp_path)

    assert context.has_operation_doc is True
    assert context.has_workflow_objective_impact is True
    assert context.has_backtest_metadata_migration is True
    assert context.has_backtest_metadata_code_writes is False
    assert context.has_objective_store is False
    assert context.active_objective_files == ()
    assert context.next_migration_number == 5
    assert context.unchecked_plan_items == (
        "Backtest includes costs, baselines, regimes, and label-scramble",
    )


def test_propose_plan_returns_objective_packets_with_ticket_impacts(
    tmp_path: Path,
) -> None:
    _write_minimal_repo(tmp_path)

    proposal = planning_steward.propose_plan(root=tmp_path, max_objectives=3)

    objective_ids = [objective.objective_id for objective in proposal.objectives]
    assert objective_ids[:3] == [
        "wire-backtest-metadata-registry",
        "create-objective-store",
        "reconcile-phase-1-plan-state",
    ]
    assert proposal.objectives[0].source.kind == "repo_heuristic"
    first_ticket = proposal.objectives[0].expected_tickets[0]
    assert "persistence layer" in first_ticket.objective_impact
    assert first_ticket.technical_summary
    assert "db/migrations/" in first_ticket.do_not_touch
    assert first_ticket.ticket_role == "implementation"
    assert first_ticket.dependency_group == "default"
    assert first_ticket.contracts_touched == ()
    assert first_ticket.risk_class == ""
    assert first_ticket.proof_packet == ()


def test_markdown_output_is_user_readable_and_propose_only(tmp_path: Path) -> None:
    _write_minimal_repo(tmp_path)
    proposal = planning_steward.propose_plan(root=tmp_path, max_objectives=1)

    output = planning_steward.render_proposal(proposal, output_format="markdown")

    assert "# Planning Steward Proposal" in output
    assert "No Linear, GitHub, database, or vendor writes were performed." in output
    assert "Ticket Role:" in output
    assert "Objective Impact:" in output
    assert "Migration Lane" in output
    assert "Source: repo heuristic" in output
    assert "wire-backtest-metadata-registry" in output


def test_json_output_contains_stable_objective_shape(tmp_path: Path) -> None:
    _write_minimal_repo(tmp_path)
    proposal = planning_steward.propose_plan(root=tmp_path, max_objectives=1)

    payload = json.loads(
        planning_steward.render_proposal(proposal, output_format="json")
    )

    assert payload["status"] == "PROPOSE_ONLY"
    assert payload["objectives"][0]["objective_id"] == (
        "wire-backtest-metadata-registry"
    )
    assert payload["objectives"][0]["source"] == {
        "objective_id": "wire-backtest-metadata-registry",
        "path": None,
        "type": "repo_heuristic",
    }
    first_ticket = payload["objectives"][0]["expected_tickets"][0]
    assert "objective_impact" in first_ticket
    assert first_ticket["ticket_role"] == "implementation"
    assert first_ticket["dependency_group"] == "default"
    assert first_ticket["contracts_touched"] == []
    assert first_ticket["risk_class"] == ""
    assert first_ticket["proof_packet"] == []


def test_active_objective_file_produces_source_backed_ticket_proposal(
    tmp_path: Path,
) -> None:
    _write_minimal_repo(tmp_path)
    objective_path = _write_active_objective(
        tmp_path,
        "approved-runtime-loop.md",
        expected_tickets="\n".join(
            [
                "- Teach steward to read active Objectives",
                "  without losing wrapped titles",
                "  Ticket Role: implementation",
                "  Dependency Group: orchestration-core",
                "  Contracts Touched:",
                "  - objective-compiler",
                "  Risk Class: low",
                "  Purpose: Parse approved Objective markdown before heuristics.",
                "  Objective Impact: Michael-approved Objective files become the planning source of truth.",
                "  Technical Summary: Add deterministic markdown parsing for active Objective files.",
                "  Owns:",
                "  - scripts/planning_steward.py",
                "  - tests/test_planning_steward.py",
                "  Do Not Touch:",
                "  - db/migrations/",
                "  Dependencies:",
                "  - Objective store ticket",
                "  Conflict Zones:",
                "  - scripts/planning_steward.py",
                "  Validation:",
                "  - python -m pytest tests/test_planning_steward.py",
                "  Proof Packet:",
                "  - PR link and validation output",
            ]
        ),
    )

    proposal = planning_steward.propose_plan(root=tmp_path, max_objectives=1)

    objective = proposal.objectives[0]
    assert objective.objective_id == "approved-runtime-loop"
    assert objective.source.kind == "objective_file"
    assert objective.source.path == objective_path.as_posix()
    ticket = objective.expected_tickets[0]
    assert ticket.title == (
        "Teach steward to read active Objectives without losing wrapped titles"
    )
    assert ticket.ticket_role == "implementation"
    assert ticket.dependency_group == "orchestration-core"
    assert ticket.contracts_touched == ("objective-compiler",)
    assert ticket.risk_class == "low"
    assert ticket.objective_impact == (
        "Michael-approved Objective files become the planning source of truth."
    )
    assert ticket.technical_summary == (
        "Add deterministic markdown parsing for active Objective files."
    )
    assert ticket.owns == (
        "scripts/planning_steward.py",
        "tests/test_planning_steward.py",
    )
    assert ticket.do_not_touch == ("db/migrations/",)
    assert ticket.dependencies == ("Objective store ticket",)
    assert ticket.conflict_zones == ("scripts/planning_steward.py",)
    assert ticket.validation == (
        "python -m pytest tests/test_planning_steward.py",
    )
    assert ticket.proof_packet == ("PR link and validation output",)


def test_json_output_contains_objective_file_source_metadata(tmp_path: Path) -> None:
    _write_minimal_repo(tmp_path)
    objective_path = _write_active_objective(tmp_path, "source-metadata.md")
    proposal = planning_steward.propose_plan(root=tmp_path, max_objectives=1)

    payload = json.loads(
        planning_steward.render_proposal(proposal, output_format="json")
    )

    assert payload["objectives"][0]["source"] == {
        "objective_id": "source-metadata",
        "path": objective_path.as_posix(),
        "type": "objective_file",
    }


def test_missing_required_objective_sections_fail_closed(tmp_path: Path) -> None:
    _write_minimal_repo(tmp_path)
    active_dir = tmp_path / "docs" / "objectives" / "active"
    completed_dir = tmp_path / "docs" / "objectives" / "completed"
    active_dir.mkdir(parents=True)
    completed_dir.mkdir(parents=True)
    (active_dir / "missing-validation.md").write_text(
        "\n".join(
            [
                "# Missing Validation",
                "",
                "Objective:",
                "Prove invalid Objective files do not silently create tickets.",
                "",
                "User Value:",
                "Michael sees bad Objective files before they become work.",
                "",
                "Why Now:",
                "The steward is becoming Objective-file aware.",
                "",
                "Done When:",
                "- The steward reports missing required sections.",
                "",
                "Out Of Scope:",
                "- No Linear writes.",
                "",
                "Guardrails:",
                "- Fail closed on incomplete Objective files.",
                "",
                "Expected Tickets:",
                "- Add validation warning",
                "",
                "Conflict Zones:",
                "- scripts/planning_steward.py",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(planning_steward.PlanningStewardError) as exc_info:
        planning_steward.propose_plan(root=tmp_path, max_objectives=1)

    assert "docs/objectives/active/missing-validation.md" in str(exc_info.value)
    assert "Validation" in str(exc_info.value)


def test_heuristic_proposals_still_work_when_objective_store_is_empty(
    tmp_path: Path,
) -> None:
    _write_minimal_repo(tmp_path)
    (tmp_path / "docs" / "objectives" / "active").mkdir(parents=True)
    (tmp_path / "docs" / "objectives" / "completed").mkdir(parents=True)

    proposal = planning_steward.propose_plan(root=tmp_path, max_objectives=1)

    assert proposal.objectives[0].objective_id == "wire-backtest-metadata-registry"
    assert proposal.objectives[0].source.kind == "repo_heuristic"


def test_check_mode_prints_success_for_available_proposals(
    tmp_path: Path,
    capsys,
) -> None:
    _write_minimal_repo(tmp_path)

    exit_code = planning_steward.main(["--check", "--root", str(tmp_path)])

    assert exit_code == 0
    assert "OK: planning steward proposal check passed" in capsys.readouterr().out


def _write_active_objective(
    root: Path,
    filename: str,
    *,
    expected_tickets: str = "- Teach steward to read active Objectives",
) -> Path:
    active_dir = root / "docs" / "objectives" / "active"
    completed_dir = root / "docs" / "objectives" / "completed"
    active_dir.mkdir(parents=True, exist_ok=True)
    completed_dir.mkdir(parents=True, exist_ok=True)
    objective_path = active_dir / filename
    objective_path.write_text(
        "\n".join(
            [
                f"# {filename.removesuffix('.md')}",
                "",
                "Objective:",
                "Teach the planning steward to read approved Objective files.",
                "",
                "User Value:",
                "Michael and Codex can agree on an Objective before tickets are proposed.",
                "",
                "Why Now:",
                "The Objective store exists and should drive the planning loop.",
                "",
                "Done When:",
                "- Active Objective files produce proposal output.",
                "- Fallback heuristics still work when no active Objective files exist.",
                "",
                "Out Of Scope:",
                "- No Linear ticket creation.",
                "",
                "Guardrails:",
                "- Keep the steward read-only.",
                "",
                "Expected Tickets:",
                expected_tickets,
                "",
                "Validation:",
                "- git diff --check",
                "- python -m pytest tests/test_planning_steward.py",
                "",
                "Conflict Zones:",
                "- scripts/planning_steward.py",
                "- tests/test_planning_steward.py",
            ]
        ),
        encoding="utf-8",
    )
    return objective_path.relative_to(root)


def _write_minimal_repo(root: Path) -> None:
    (root / "docs" / "exec-plans" / "active").mkdir(parents=True)
    (root / "docs" / "exec-plans" / "active" / "phase-1-foundation.md").write_text(
        "\n".join(
            [
                "# Phase 1 Foundation Plan",
                "",
                "## Acceptance Criteria",
                "",
                "- [x] Trading calendar is seeded for 2014-2026",
                "- [ ] Backtest includes costs, baselines, regimes, and label-scramble",
            ]
        ),
        encoding="utf-8",
    )
    (root / "docs" / "Symphony-Operation.md").write_text(
        "# Silver Symphony Operation\n\nPlanning Steward\nObjective Impact\n",
        encoding="utf-8",
    )
    (root / "WORKFLOW.md").write_text(
        "## Proof Packet\n\n- Objective Impact\n",
        encoding="utf-8",
    )
    (root / "db" / "migrations").mkdir(parents=True)
    for filename in (
        "001_foundation.sql",
        "002_raw_objects_metadata.sql",
        "003_phase1_analytics.sql",
        "004_backtest_metadata.sql",
    ):
        (root / "db" / "migrations" / filename).write_text("-- test\n")
    (root / "src").mkdir()
    (root / "scripts").mkdir()
    (root / "scripts" / "planning_steward.py").write_text("# test\n")
