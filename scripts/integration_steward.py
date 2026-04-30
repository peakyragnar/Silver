#!/usr/bin/env python
"""Create repair packets for Rework tickets after VCS reconciliation."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import work_ledger  # noqa: E402


ACTOR = "integration_steward"
EVENT_TYPE = "integration_repair_requested"
REPAIR_TARGET_STATUS = work_ledger.TICKET_STATUS_REWORK

RepairKind = Literal[
    "merge_conflict",
    "failed_check",
    "closed_pr",
    "missing_pr_evidence",
    "general_rework",
]
OutputFormat = Literal["text", "json"]


class IntegrationStewardError(RuntimeError):
    """Raised when integration repair planning cannot safely continue."""


@dataclass(frozen=True, slots=True)
class RepairPacket:
    ticket_id: str
    objective_id: str
    title: str
    repair_kind: RepairKind
    blocker: str
    pr_url: str | None
    branch: str | None
    owns: tuple[str, ...]
    do_not_touch: tuple[str, ...]
    validation: tuple[str, ...]
    proof_packet: tuple[str, ...]
    body: str


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--ledger",
        type=Path,
        default=work_ledger.default_ledger_path(),
        help="ledger path; defaults to SILVER_LEDGER_PATH or .silver/work_ledger.db",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="record repair packets in the ledger audit trail",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="output format",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate local configuration without writes",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.check:
            with work_ledger.connect_existing(args.ledger):
                pass
            print(
                "\n".join(
                    (
                        "Integration steward configuration check",
                        "",
                        f"Ledger: {args.ledger}",
                        "Result: local integration steward configuration is valid",
                    )
                )
            )
            return 0

        with work_ledger.connect_existing(args.ledger) as connection:
            packets = repair_packets(connection)
            if args.apply:
                record_repair_packets(connection, packets)
        print(render_packets(packets, output_format=args.format, applied=args.apply))
    except (IntegrationStewardError, work_ledger.WorkLedgerError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


def repair_packets(connection: sqlite3.Connection) -> tuple[RepairPacket, ...]:
    rows = connection.execute(
        """
        SELECT
          tickets.*,
          (
            SELECT message
            FROM ticket_events
            WHERE ticket_events.ticket_id = tickets.id
              AND ticket_events.actor != ?
            ORDER BY ticket_events.id DESC
            LIMIT 1
          ) AS latest_blocker
        FROM tickets
        WHERE tickets.status = ?
        ORDER BY tickets.objective_id, tickets.sequence
        """,
        (ACTOR, REPAIR_TARGET_STATUS),
    ).fetchall()
    return tuple(row_to_repair_packet(row) for row in rows)


def row_to_repair_packet(row: sqlite3.Row) -> RepairPacket:
    blocker = str(row["latest_blocker"] or "Rework requested; no blocker recorded.")
    pr_url = row["pr_url"]
    branch = row["branch"]
    owns = tuple(work_ledger.loads_json(row["owns_json"]))
    do_not_touch = tuple(work_ledger.loads_json(row["do_not_touch_json"]))
    validation = tuple(work_ledger.loads_json(row["validation_json"]))
    proof_packet = tuple(work_ledger.loads_json(row["proof_packet_json"]))
    repair_kind = classify_repair_kind(blocker, pr_url=pr_url, branch=branch)
    packet = RepairPacket(
        ticket_id=row["id"],
        objective_id=row["objective_id"],
        title=row["title"],
        repair_kind=repair_kind,
        blocker=blocker,
        pr_url=pr_url,
        branch=branch,
        owns=owns,
        do_not_touch=do_not_touch,
        validation=validation,
        proof_packet=proof_packet,
        body="",
    )
    return RepairPacket(
        ticket_id=packet.ticket_id,
        objective_id=packet.objective_id,
        title=packet.title,
        repair_kind=packet.repair_kind,
        blocker=packet.blocker,
        pr_url=packet.pr_url,
        branch=packet.branch,
        owns=packet.owns,
        do_not_touch=packet.do_not_touch,
        validation=packet.validation,
        proof_packet=packet.proof_packet,
        body=render_repair_packet(packet),
    )


def classify_repair_kind(
    blocker: str,
    *,
    pr_url: str | None,
    branch: str | None,
) -> RepairKind:
    if not pr_url or not branch:
        return "missing_pr_evidence"
    lowered = blocker.lower()
    if "merge conflict" in lowered or "mergeable_state=dirty" in lowered:
        return "merge_conflict"
    if "failed required check" in lowered or "failed check" in lowered:
        return "failed_check"
    if "closed without merge" in lowered:
        return "closed_pr"
    return "general_rework"


def render_repair_packet(packet: RepairPacket) -> str:
    lines = [
        "## Integration Repair Packet",
        "",
        f"Objective: {packet.objective_id}",
        f"Ticket: {packet.ticket_id} - {packet.title}",
        f"Repair Kind: {packet.repair_kind}",
        f"PR: {packet.pr_url or 'missing'}",
        f"Branch: {packet.branch or 'missing'}",
        "",
        "Blocker:",
        packet.blocker,
        "",
        "Allowed Scope:",
        *bullet_lines(packet.owns),
        "",
        "Do Not Touch:",
        *bullet_lines(packet.do_not_touch),
        "",
        "Repair Instructions:",
        *repair_instruction_lines(packet),
        "",
        "Validation Required:",
        *bullet_lines(packet.validation),
        "",
        "Proof Packet Refresh:",
        *bullet_lines(packet.proof_packet),
    ]
    return "\n".join(lines).strip()


def repair_instruction_lines(packet: RepairPacket) -> list[str]:
    if packet.repair_kind == "missing_pr_evidence":
        return [
            "- Find or attach the matching PR before code repair.",
            "- Do not guess the branch from ticket text alone.",
            "- Re-run VCS reconciliation after PR evidence is available.",
        ]
    if packet.repair_kind == "merge_conflict":
        return [
            "- Start from the listed PR branch and update it against current main.",
            "- Resolve only the mechanical conflict that caused Rework.",
            "- Route semantic contract drift to Safety Review instead of patching it.",
            "- After repair, refresh proof and move the ticket back to Merging.",
        ]
    if packet.repair_kind == "failed_check":
        return [
            "- Start from the listed PR branch and inspect the failing check output.",
            "- Repair only the failure that caused Rework.",
            "- Do not broaden product scope while fixing CI.",
            "- After checks pass, refresh proof and move the ticket back to Merging.",
        ]
    if packet.repair_kind == "closed_pr":
        return [
            "- Determine whether the PR was closed intentionally.",
            "- If work remains valid, reopen or create a replacement branch.",
            "- If the work is obsolete, route the ticket to Canceled or Safety Review.",
        ]
    return [
        "- Start from the listed PR branch and repair the recorded blocker.",
        "- Keep changes inside the ticket's allowed scope.",
        "- Refresh proof and move the ticket back to Merging when complete.",
    ]


def bullet_lines(items: Sequence[str]) -> list[str]:
    if not items:
        return ["- none"]
    return [f"- `{item}`" for item in items]


def record_repair_packets(
    connection: sqlite3.Connection,
    packets: Sequence[RepairPacket],
) -> None:
    now = work_ledger.utc_now()
    with connection:
        for packet in packets:
            if repair_packet_already_recorded(connection, packet):
                continue
            work_ledger.insert_event(
                connection,
                ticket_id=packet.ticket_id,
                objective_id=packet.objective_id,
                event_type=EVENT_TYPE,
                from_status=REPAIR_TARGET_STATUS,
                to_status=REPAIR_TARGET_STATUS,
                message=packet.body,
                actor=ACTOR,
                created_at=now,
            )


def repair_packet_already_recorded(
    connection: sqlite3.Connection,
    packet: RepairPacket,
) -> bool:
    row = connection.execute(
        """
        SELECT message FROM ticket_events
        WHERE ticket_id = ?
          AND event_type = ?
          AND actor = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (packet.ticket_id, EVENT_TYPE, ACTOR),
    ).fetchone()
    return row is not None and row["message"] == packet.body


def render_packets(
    packets: Sequence[RepairPacket],
    *,
    output_format: OutputFormat,
    applied: bool,
) -> str:
    payload = {
        "applied": applied,
        "repair_packets": [packet_payload(packet) for packet in packets],
    }
    if output_format == "json":
        return json.dumps(payload, indent=2, sort_keys=True)
    prefix = "APPLIED" if applied else "DRY RUN"
    if not packets:
        return f"{prefix}: no Rework tickets need repair packets."
    lines = [f"{prefix}: integration repair packets"]
    for packet in packets:
        pr = packet.pr_url or "missing PR"
        lines.append(
            f"- {packet.ticket_id} | {packet.repair_kind} | {pr} | "
            f"{packet.blocker.splitlines()[0]}"
        )
    return "\n".join(lines)


def packet_payload(packet: RepairPacket) -> Mapping[str, object]:
    return {
        "ticket_id": packet.ticket_id,
        "objective_id": packet.objective_id,
        "title": packet.title,
        "repair_kind": packet.repair_kind,
        "blocker": packet.blocker,
        "pr_url": packet.pr_url,
        "branch": packet.branch,
        "owns": list(packet.owns),
        "do_not_touch": list(packet.do_not_touch),
        "validation": list(packet.validation),
        "proof_packet": list(packet.proof_packet),
        "body": packet.body,
    }


if __name__ == "__main__":
    raise SystemExit(main())
