#!/usr/bin/env python
"""Manage Silver's local Objective and ticket work ledger."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import planning_steward  # noqa: E402


DEFAULT_LEDGER_PATH = ROOT / ".silver" / "work_ledger.db"
LEDGER_ENV_VAR = "SILVER_LEDGER_PATH"
SCHEMA_VERSION = 1

OBJECTIVE_STATUS_ACTIVE = "active"
TICKET_STATUS_BACKLOG = "Backlog"
TICKET_STATUS_READY = "Ready"
TICKET_STATUS_REWORK = "Rework"
TICKET_TERMINAL_STATUSES = frozenset(("Done", "Canceled", "Duplicate"))
TICKET_ACTIVE_STATUSES = frozenset(("Ready", "Claimed", "In Progress", "Rework"))
TICKET_STATUSES = frozenset(
    (
        "Backlog",
        "Ready",
        "Claimed",
        "In Progress",
        "Rework",
        "Merging",
        "Done",
        "Safety Review",
        "Blocked",
        "Canceled",
        "Duplicate",
    )
)

OutputFormat = Literal["text", "json"]


class WorkLedgerError(RuntimeError):
    """Raised when the local work ledger cannot safely continue."""


@dataclass(frozen=True, slots=True)
class LedgerTicket:
    id: str
    objective_id: str
    sequence: int
    title: str
    status: str
    objective_impact: str
    conflict_domain: str
    dependencies: tuple[str, ...]
    owns: tuple[str, ...]
    validation: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ImportResult:
    objectives_imported: int
    tickets_created: int
    tickets_updated: int


@dataclass(frozen=True, slots=True)
class AdmissionResult:
    promoted: tuple[LedgerTicket, ...]
    waiting: tuple[tuple[LedgerTicket, str], ...]


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--ledger",
        type=Path,
        default=default_ledger_path(),
        help=f"ledger path; defaults to ${LEDGER_ENV_VAR} or {DEFAULT_LEDGER_PATH}",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=ROOT,
        help="repository root; intended for tests",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init", help="create or upgrade the ledger")
    init_parser.set_defaults(func=command_init)

    status_parser = subparsers.add_parser("status", help="summarize ledger state")
    add_format_arg(status_parser)
    status_parser.set_defaults(func=command_status)

    import_parser = subparsers.add_parser(
        "import-objectives",
        help="import approved active Objective files into Backlog tickets",
    )
    import_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="parse and report counts without writing to the ledger",
    )
    add_format_arg(import_parser)
    import_parser.set_defaults(func=command_import_objectives)

    runnable_parser = subparsers.add_parser(
        "list-runnable",
        help="list Ready/Rework tickets with satisfied local dependencies",
    )
    add_format_arg(runnable_parser)
    runnable_parser.set_defaults(func=command_list_runnable)

    admit_parser = subparsers.add_parser(
        "admit",
        help="promote Backlog tickets to Ready within local capacity limits",
    )
    admit_parser.add_argument("--max-active", type=int, default=5)
    admit_parser.add_argument("--ready-buffer", type=int, default=5)
    admit_parser.add_argument("--dry-run", action="store_true")
    add_format_arg(admit_parser)
    admit_parser.set_defaults(func=command_admit)

    transition_parser = subparsers.add_parser(
        "transition",
        help="move one local ledger ticket to a new status",
    )
    transition_parser.add_argument("ticket_id")
    transition_parser.add_argument("status")
    transition_parser.add_argument("--message", default="")
    transition_parser.add_argument("--actor", default="work_ledger")
    add_format_arg(transition_parser)
    transition_parser.set_defaults(func=command_transition)

    events_parser = subparsers.add_parser(
        "events",
        help="show recent ticket events",
    )
    events_parser.add_argument("--ticket-id", default="")
    events_parser.add_argument("--limit", type=int, default=20)
    add_format_arg(events_parser)
    events_parser.set_defaults(func=command_events)

    return parser.parse_args(argv)


def add_format_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="output format",
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = args.func(args)
        if result is not None:
            print(result)
    except WorkLedgerError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


def command_init(args: argparse.Namespace) -> str:
    initialize_ledger(args.ledger)
    return f"OK: work ledger initialized at {args.ledger}"


def command_status(args: argparse.Namespace) -> str:
    with connect_existing(args.ledger) as connection:
        payload = ledger_status(connection, args.ledger)
    return render_status(payload, args.format)


def command_import_objectives(args: argparse.Namespace) -> str:
    objectives = active_objective_proposals(args.root)
    if args.dry_run:
        result = ImportResult(
            objectives_imported=len(objectives),
            tickets_created=sum(len(objective.expected_tickets) for objective in objectives),
            tickets_updated=0,
        )
    else:
        with connect_initialized(args.ledger) as connection:
            result = import_objectives(connection, objectives)
    return render_import_result(result, args.format)


def command_list_runnable(args: argparse.Namespace) -> str:
    with connect_existing(args.ledger) as connection:
        tickets = runnable_tickets(connection)
    return render_tickets(tickets, args.format)


def command_admit(args: argparse.Namespace) -> str:
    validate_capacity_args(args.max_active, args.ready_buffer)
    with connect_existing(args.ledger) as connection:
        result = admit_backlog_tickets(
            connection,
            max_active=args.max_active,
            ready_buffer=args.ready_buffer,
            dry_run=args.dry_run,
        )
    return render_admission_result(result, args.format, dry_run=args.dry_run)


def command_transition(args: argparse.Namespace) -> str:
    if args.status not in TICKET_STATUSES:
        raise WorkLedgerError(
            "status must be one of: " + ", ".join(sorted(TICKET_STATUSES))
        )
    with connect_existing(args.ledger) as connection:
        ticket = transition_ticket(
            connection,
            ticket_id=args.ticket_id,
            status=args.status,
            actor=args.actor,
            message=args.message,
        )
    return render_transition(ticket, args.format)


def command_events(args: argparse.Namespace) -> str:
    validate_positive_int(args.limit, "limit")
    with connect_existing(args.ledger) as connection:
        events = recent_events(
            connection,
            ticket_id=args.ticket_id or None,
            limit=args.limit,
        )
    return render_events(events, args.format)


def default_ledger_path() -> Path:
    configured = os.environ.get(LEDGER_ENV_VAR)
    if configured:
        return Path(configured).expanduser()
    return DEFAULT_LEDGER_PATH


def connect_initialized(path: Path) -> sqlite3.Connection:
    initialize_ledger(path)
    return connect(path)


def connect_existing(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise WorkLedgerError(
            f"ledger does not exist: {path}. Run `scripts/work_ledger.py init` first."
        )
    connection = connect(path)
    ensure_schema_current(connection)
    return connection


def connect(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 5000")
    return connection


def initialize_ledger(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with connect(path) as connection:
        apply_schema(connection)
        ensure_schema_current(connection)


def apply_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS objectives (
          id TEXT PRIMARY KEY,
          title TEXT NOT NULL,
          user_value TEXT NOT NULL,
          why_now TEXT NOT NULL,
          source_path TEXT,
          status TEXT NOT NULL,
          validation_json TEXT NOT NULL,
          conflict_zones_json TEXT NOT NULL,
          imported_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS tickets (
          id TEXT PRIMARY KEY,
          objective_id TEXT NOT NULL REFERENCES objectives(id) ON DELETE CASCADE,
          sequence INTEGER NOT NULL,
          title TEXT NOT NULL,
          purpose TEXT NOT NULL,
          objective_impact TEXT NOT NULL,
          technical_summary TEXT NOT NULL,
          status TEXT NOT NULL,
          risk_class TEXT NOT NULL,
          conflict_domain TEXT NOT NULL,
          owns_json TEXT NOT NULL,
          do_not_touch_json TEXT NOT NULL,
          dependencies_json TEXT NOT NULL,
          conflict_zones_json TEXT NOT NULL,
          validation_json TEXT NOT NULL,
          branch TEXT,
          pr_url TEXT,
          linear_identifier TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          UNIQUE(objective_id, sequence)
        );

        CREATE TABLE IF NOT EXISTS ticket_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ticket_id TEXT,
          objective_id TEXT,
          event_type TEXT NOT NULL,
          from_status TEXT,
          to_status TEXT,
          message TEXT NOT NULL,
          actor TEXT NOT NULL,
          created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS runs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ticket_id TEXT NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
          agent_id TEXT,
          branch TEXT,
          started_at TEXT NOT NULL,
          finished_at TEXT,
          status TEXT NOT NULL,
          last_error TEXT
        );

        CREATE TABLE IF NOT EXISTS conflict_locks (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ticket_id TEXT NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
          domain TEXT NOT NULL,
          path TEXT,
          acquired_at TEXT NOT NULL,
          released_at TEXT,
          UNIQUE(domain, path, released_at)
        );

        CREATE TABLE IF NOT EXISTS proof_packets (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ticket_id TEXT NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
          pr_url TEXT,
          body TEXT NOT NULL,
          validation_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS safety_stops (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ticket_id TEXT NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
          reason TEXT NOT NULL,
          details TEXT NOT NULL,
          created_at TEXT NOT NULL,
          resolved_at TEXT
        );

        CREATE TABLE IF NOT EXISTS linear_mirror_state (
          ticket_id TEXT PRIMARY KEY REFERENCES tickets(id) ON DELETE CASCADE,
          linear_identifier TEXT,
          linear_state TEXT,
          last_synced_at TEXT,
          last_error TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_tickets_status ON tickets(status);
        CREATE INDEX IF NOT EXISTS idx_tickets_objective ON tickets(objective_id);
        CREATE INDEX IF NOT EXISTS idx_ticket_events_ticket ON ticket_events(ticket_id);
        """
    )
    connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")


def ensure_schema_current(connection: sqlite3.Connection) -> None:
    version = int(connection.execute("PRAGMA user_version").fetchone()[0])
    if version != SCHEMA_VERSION:
        raise WorkLedgerError(
            f"unsupported work ledger schema version {version}; expected {SCHEMA_VERSION}"
        )


def active_objective_proposals(
    root: Path,
) -> tuple[planning_steward.ObjectiveProposal, ...]:
    proposal = planning_steward.propose_plan(root=root, max_objectives=1000)
    objectives = tuple(
        objective
        for objective in proposal.objectives
        if objective.source.kind == "objective_file"
    )
    if not objectives:
        raise WorkLedgerError(
            "no active Objective files found to import from docs/objectives/active"
        )
    return objectives


def import_objectives(
    connection: sqlite3.Connection,
    objectives: Sequence[planning_steward.ObjectiveProposal],
) -> ImportResult:
    now = utc_now()
    objectives_imported = 0
    tickets_created = 0
    tickets_updated = 0

    with connection:
        for objective in objectives:
            upsert_objective(connection, objective, now)
            objectives_imported += 1
            for sequence, ticket in enumerate(objective.expected_tickets, start=1):
                ticket_id = ledger_ticket_id(objective.objective_id, sequence)
                created = upsert_ticket(
                    connection,
                    objective=objective,
                    ticket=ticket,
                    ticket_id=ticket_id,
                    sequence=sequence,
                    now=now,
                )
                if created:
                    tickets_created += 1
                    insert_event(
                        connection,
                        ticket_id=ticket_id,
                        objective_id=objective.objective_id,
                        event_type="ticket_imported",
                        from_status=None,
                        to_status=TICKET_STATUS_BACKLOG,
                        message="Imported from active Objective file.",
                        actor="work_ledger",
                        created_at=now,
                    )
                else:
                    tickets_updated += 1
        return ImportResult(objectives_imported, tickets_created, tickets_updated)


def upsert_objective(
    connection: sqlite3.Connection,
    objective: planning_steward.ObjectiveProposal,
    now: str,
) -> None:
    existing = connection.execute(
        "SELECT imported_at FROM objectives WHERE id = ?",
        (objective.objective_id,),
    ).fetchone()
    imported_at = existing["imported_at"] if existing is not None else now
    connection.execute(
        """
        INSERT INTO objectives (
          id, title, user_value, why_now, source_path, status, validation_json,
          conflict_zones_json, imported_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
          title = excluded.title,
          user_value = excluded.user_value,
          why_now = excluded.why_now,
          source_path = excluded.source_path,
          status = excluded.status,
          validation_json = excluded.validation_json,
          conflict_zones_json = excluded.conflict_zones_json,
          updated_at = excluded.updated_at
        """,
        (
            objective.objective_id,
            objective.objective,
            objective.user_value,
            objective.why_now,
            objective.source.path,
            OBJECTIVE_STATUS_ACTIVE,
            dumps_json(objective.validation),
            dumps_json(objective.conflict_zones),
            imported_at,
            now,
        ),
    )


def upsert_ticket(
    connection: sqlite3.Connection,
    *,
    objective: planning_steward.ObjectiveProposal,
    ticket: planning_steward.TicketProposal,
    ticket_id: str,
    sequence: int,
    now: str,
) -> bool:
    existing = connection.execute(
        "SELECT status, created_at FROM tickets WHERE id = ?",
        (ticket_id,),
    ).fetchone()
    created = existing is None
    status = TICKET_STATUS_BACKLOG if existing is None else existing["status"]
    created_at = now if existing is None else existing["created_at"]
    conflict_domain = infer_conflict_domain(ticket)
    risk_class = infer_risk_class(ticket, conflict_domain)
    connection.execute(
        """
        INSERT INTO tickets (
          id, objective_id, sequence, title, purpose, objective_impact,
          technical_summary, status, risk_class, conflict_domain, owns_json,
          do_not_touch_json, dependencies_json, conflict_zones_json,
          validation_json, branch, pr_url, linear_identifier, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
          title = excluded.title,
          purpose = excluded.purpose,
          objective_impact = excluded.objective_impact,
          technical_summary = excluded.technical_summary,
          risk_class = excluded.risk_class,
          conflict_domain = excluded.conflict_domain,
          owns_json = excluded.owns_json,
          do_not_touch_json = excluded.do_not_touch_json,
          dependencies_json = excluded.dependencies_json,
          conflict_zones_json = excluded.conflict_zones_json,
          validation_json = excluded.validation_json,
          updated_at = excluded.updated_at
        """,
        (
            ticket_id,
            objective.objective_id,
            sequence,
            ticket.title,
            ticket.purpose,
            ticket.objective_impact,
            ticket.technical_summary,
            status,
            risk_class,
            conflict_domain,
            dumps_json(ticket.owns),
            dumps_json(ticket.do_not_touch),
            dumps_json(ticket.dependencies),
            dumps_json(ticket.conflict_zones),
            dumps_json(ticket.validation),
            created_at,
            now,
        ),
    )
    return created


def admit_backlog_tickets(
    connection: sqlite3.Connection,
    *,
    max_active: int,
    ready_buffer: int,
    dry_run: bool,
) -> AdmissionResult:
    active_count = scalar_int(
        connection,
        f"""
        SELECT COUNT(*) FROM tickets
        WHERE status IN ({placeholders(TICKET_ACTIVE_STATUSES)})
        """,
        tuple(TICKET_ACTIVE_STATUSES),
    )
    ready_count = scalar_int(
        connection,
        "SELECT COUNT(*) FROM tickets WHERE status = ?",
        (TICKET_STATUS_READY,),
    )
    slots = min(max_active - active_count, ready_buffer - ready_count)
    if slots <= 0:
        waiting = tuple(
            (ticket, "capacity full")
            for ticket in backlog_tickets(connection)
        )
        return AdmissionResult((), waiting)

    promoted: list[LedgerTicket] = []
    waiting: list[tuple[LedgerTicket, str]] = []
    now = utc_now()
    with connection:
        active_after_promotions = list(active_tickets(connection))
        for ticket in backlog_tickets(connection):
            if slots <= 0:
                waiting.append((ticket, "capacity full"))
                continue
            blocker = runnable_blocker(ticket, active_after_promotions, connection)
            if blocker is not None:
                waiting.append((ticket, blocker))
                continue
            if not dry_run:
                update_ticket_status(
                    connection,
                    ticket=ticket,
                    status=TICKET_STATUS_READY,
                    actor="work_ledger",
                    message="Local ledger admission promoted Backlog to Ready.",
                    created_at=now,
                )
            promoted_ticket = replace_ticket_status(ticket, TICKET_STATUS_READY)
            promoted.append(promoted_ticket)
            active_after_promotions.append(promoted_ticket)
            slots -= 1
    return AdmissionResult(tuple(promoted), tuple(waiting))


def runnable_tickets(connection: sqlite3.Connection) -> tuple[LedgerTicket, ...]:
    tickets = select_tickets(
        connection,
        f"""
        SELECT * FROM tickets
        WHERE status IN ({placeholders((TICKET_STATUS_READY, TICKET_STATUS_REWORK))})
        ORDER BY objective_id, sequence
        """,
        (TICKET_STATUS_READY, TICKET_STATUS_REWORK),
    )
    return tuple(
        ticket
        for ticket in tickets
        if dependency_blocker(ticket, connection) is None
    )


def transition_ticket(
    connection: sqlite3.Connection,
    *,
    ticket_id: str,
    status: str,
    actor: str,
    message: str,
) -> LedgerTicket:
    ticket = select_ticket(connection, ticket_id)
    if ticket.status == status:
        return ticket
    with connection:
        update_ticket_status(
            connection,
            ticket=ticket,
            status=status,
            actor=actor,
            message=message or f"Moved {ticket_id} to {status}.",
            created_at=utc_now(),
        )
    return replace_ticket_status(ticket, status)


def update_ticket_status(
    connection: sqlite3.Connection,
    *,
    ticket: LedgerTicket,
    status: str,
    actor: str,
    message: str,
    created_at: str,
) -> None:
    connection.execute(
        "UPDATE tickets SET status = ?, updated_at = ? WHERE id = ?",
        (status, created_at, ticket.id),
    )
    insert_event(
        connection,
        ticket_id=ticket.id,
        objective_id=ticket.objective_id,
        event_type="status_transition",
        from_status=ticket.status,
        to_status=status,
        message=message,
        actor=actor,
        created_at=created_at,
    )


def insert_event(
    connection: sqlite3.Connection,
    *,
    ticket_id: str | None,
    objective_id: str | None,
    event_type: str,
    from_status: str | None,
    to_status: str | None,
    message: str,
    actor: str,
    created_at: str,
) -> None:
    connection.execute(
        """
        INSERT INTO ticket_events (
          ticket_id, objective_id, event_type, from_status, to_status, message,
          actor, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ticket_id,
            objective_id,
            event_type,
            from_status,
            to_status,
            message,
            actor,
            created_at,
        ),
    )


def ledger_status(connection: sqlite3.Connection, path: Path) -> dict[str, object]:
    objectives = scalar_int(connection, "SELECT COUNT(*) FROM objectives", ())
    tickets = scalar_int(connection, "SELECT COUNT(*) FROM tickets", ())
    rows = connection.execute(
        "SELECT status, COUNT(*) AS count FROM tickets GROUP BY status ORDER BY status"
    ).fetchall()
    return {
        "ledger": str(path),
        "schema_version": SCHEMA_VERSION,
        "objectives": objectives,
        "tickets": tickets,
        "runnable": len(runnable_tickets(connection)),
        "statuses": {row["status"]: int(row["count"]) for row in rows},
    }


def recent_events(
    connection: sqlite3.Connection,
    *,
    ticket_id: str | None,
    limit: int,
) -> tuple[Mapping[str, object], ...]:
    where = ""
    params: tuple[object, ...]
    if ticket_id:
        where = "WHERE ticket_id = ?"
        params = (ticket_id, limit)
    else:
        params = (limit,)
    rows = connection.execute(
        f"""
        SELECT ticket_id, objective_id, event_type, from_status, to_status,
               message, actor, created_at
        FROM ticket_events
        {where}
        ORDER BY id DESC
        LIMIT ?
        """,
        params,
    ).fetchall()
    return tuple(dict(row) for row in rows)


def active_tickets(connection: sqlite3.Connection) -> tuple[LedgerTicket, ...]:
    return select_tickets(
        connection,
        f"""
        SELECT * FROM tickets
        WHERE status IN ({placeholders(TICKET_ACTIVE_STATUSES)})
        ORDER BY objective_id, sequence
        """,
        tuple(TICKET_ACTIVE_STATUSES),
    )


def backlog_tickets(connection: sqlite3.Connection) -> tuple[LedgerTicket, ...]:
    return select_tickets(
        connection,
        "SELECT * FROM tickets WHERE status = ? ORDER BY objective_id, sequence",
        (TICKET_STATUS_BACKLOG,),
    )


def select_ticket(connection: sqlite3.Connection, ticket_id: str) -> LedgerTicket:
    row = connection.execute(
        "SELECT * FROM tickets WHERE id = ?",
        (ticket_id,),
    ).fetchone()
    if row is None:
        raise WorkLedgerError(f"ticket not found: {ticket_id}")
    return row_to_ticket(row)


def select_tickets(
    connection: sqlite3.Connection,
    query: str,
    params: Sequence[object],
) -> tuple[LedgerTicket, ...]:
    return tuple(row_to_ticket(row) for row in connection.execute(query, params))


def row_to_ticket(row: sqlite3.Row) -> LedgerTicket:
    return LedgerTicket(
        id=row["id"],
        objective_id=row["objective_id"],
        sequence=int(row["sequence"]),
        title=row["title"],
        status=row["status"],
        objective_impact=row["objective_impact"],
        conflict_domain=row["conflict_domain"],
        dependencies=tuple(loads_json(row["dependencies_json"])),
        owns=tuple(loads_json(row["owns_json"])),
        validation=tuple(loads_json(row["validation_json"])),
    )


def runnable_blocker(
    ticket: LedgerTicket,
    active: Sequence[LedgerTicket],
    connection: sqlite3.Connection,
) -> str | None:
    dependency = dependency_blocker(ticket, connection)
    if dependency is not None:
        return dependency
    if ticket.conflict_domain == "migration":
        for active_ticket in active:
            if active_ticket.conflict_domain == "migration":
                return f"migration lane occupied by {active_ticket.id}"
    return None


def dependency_blocker(
    ticket: LedgerTicket,
    connection: sqlite3.Connection,
) -> str | None:
    tickets_by_title = {
        row["title"].lower(): row["status"]
        for row in connection.execute("SELECT title, status FROM tickets")
    }
    tickets_by_id = {
        row["id"].lower(): row["status"]
        for row in connection.execute("SELECT id, status FROM tickets")
    }
    for dependency in ticket.dependencies:
        key = dependency.lower()
        status = tickets_by_id.get(key) or tickets_by_title.get(key)
        if status is not None and status not in TICKET_TERMINAL_STATUSES:
            return f"blocked by unfinished dependency: {dependency}"
    return None


def ledger_ticket_id(objective_id: str, sequence: int) -> str:
    return f"{objective_id}-{sequence:03d}"


def infer_conflict_domain(ticket: planning_steward.TicketProposal) -> str:
    primary_scope = " ".join((ticket.title, ticket.purpose)).lower()
    full_scope = " ".join(
        (
            ticket.title,
            ticket.purpose,
            ticket.objective_impact,
            ticket.technical_summary,
        )
    ).lower()
    normalized_paths = tuple(
        path.lower()
        for path in (*ticket.owns, *ticket.conflict_zones)
    )
    if any(token in primary_scope for token in ("docs", "documentation", "spec.md")):
        return "docs"
    if "report" in primary_scope:
        return "reporting"
    if any(token in primary_scope for token in ("schema", "migration", "migrations")):
        return "migration"
    if any(
        token in primary_scope
        for token in ("backtest", "falsifier", "walk-forward", "run metadata")
    ):
        return "backtest"
    if "report" in full_scope:
        return "reporting"
    if any(token in full_scope for token in ("schema", "migration", "migrations")):
        return "migration"
    if any(
        token in full_scope
        for token in ("backtest", "falsifier", "walk-forward", "model_run")
    ):
        return "backtest"
    if any(
        "scripts/" in path and "steward" in path
        for path in normalized_paths
    ):
        return "orchestration"
    if any(
        "db/migrations" in path or "schema" in path
        for path in normalized_paths
    ):
        return "migration"
    if any(
        "run_falsifier" in path or "backtest" in path
        for path in normalized_paths
    ):
        return "backtest"
    if any("reports/" in path for path in normalized_paths):
        return "reporting"
    if any("docs/" in path for path in normalized_paths):
        return "docs"
    return "general"


def infer_risk_class(
    ticket: planning_steward.TicketProposal,
    conflict_domain: str,
) -> str:
    haystack = " ".join(
        (
            ticket.title,
            ticket.technical_summary,
        )
    ).lower()
    if any(token in haystack for token in ("drop ", "delete ", "secret", "credential")):
        return "safety"
    if conflict_domain == "migration":
        return "migration"
    if "backtest" in haystack or "metric" in haystack:
        return "semantic"
    return "low"


def scalar_int(
    connection: sqlite3.Connection,
    query: str,
    params: Sequence[object],
) -> int:
    return int(connection.execute(query, params).fetchone()[0])


def placeholders(items: Iterable[object]) -> str:
    count = len(tuple(items))
    if count < 1:
        raise WorkLedgerError("cannot build placeholders for empty sequence")
    return ", ".join("?" for _ in range(count))


def validate_capacity_args(max_active: int, ready_buffer: int) -> None:
    validate_positive_int(max_active, "max_active")
    validate_positive_int(ready_buffer, "ready_buffer")


def validate_positive_int(value: int, name: str) -> None:
    if isinstance(value, bool) or value < 1:
        raise WorkLedgerError(f"{name} must be a positive integer")


def replace_ticket_status(ticket: LedgerTicket, status: str) -> LedgerTicket:
    return LedgerTicket(
        id=ticket.id,
        objective_id=ticket.objective_id,
        sequence=ticket.sequence,
        title=ticket.title,
        status=status,
        objective_impact=ticket.objective_impact,
        conflict_domain=ticket.conflict_domain,
        dependencies=ticket.dependencies,
        owns=ticket.owns,
        validation=ticket.validation,
    )


def dumps_json(values: Sequence[str]) -> str:
    return json.dumps(list(values), sort_keys=True)


def loads_json(value: str) -> list[str]:
    payload = json.loads(value)
    if not isinstance(payload, list):
        raise WorkLedgerError("ledger JSON column did not contain a list")
    return [str(item) for item in payload]


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def render_status(payload: Mapping[str, object], output_format: OutputFormat) -> str:
    if output_format == "json":
        return json.dumps(payload, indent=2, sort_keys=True)
    lines = [
        "Work ledger status",
        f"Ledger: {payload['ledger']}",
        f"Schema version: {payload['schema_version']}",
        f"Objectives: {payload['objectives']}",
        f"Tickets: {payload['tickets']}",
        f"Runnable: {payload['runnable']}",
        "Statuses:",
    ]
    statuses = payload["statuses"]
    if isinstance(statuses, Mapping) and statuses:
        lines.extend(f"- {status}: {count}" for status, count in statuses.items())
    else:
        lines.append("- none")
    return "\n".join(lines)


def render_import_result(result: ImportResult, output_format: OutputFormat) -> str:
    payload = {
        "objectives_imported": result.objectives_imported,
        "tickets_created": result.tickets_created,
        "tickets_updated": result.tickets_updated,
    }
    if output_format == "json":
        return json.dumps(payload, indent=2, sort_keys=True)
    return (
        "Imported active Objectives into work ledger\n"
        f"Objectives: {result.objectives_imported}\n"
        f"Tickets created: {result.tickets_created}\n"
        f"Tickets updated: {result.tickets_updated}"
    )


def render_tickets(
    tickets: Sequence[LedgerTicket],
    output_format: OutputFormat,
) -> str:
    payload = [ticket_payload(ticket) for ticket in tickets]
    if output_format == "json":
        return json.dumps(payload, indent=2, sort_keys=True)
    if not tickets:
        return "No runnable tickets."
    lines = ["Runnable tickets:"]
    for ticket in tickets:
        lines.append(
            f"- {ticket.id} | {ticket.status} | {ticket.conflict_domain} | "
            f"{ticket.title}"
        )
    return "\n".join(lines)


def render_admission_result(
    result: AdmissionResult,
    output_format: OutputFormat,
    *,
    dry_run: bool,
) -> str:
    payload = {
        "dry_run": dry_run,
        "promoted": [ticket_payload(ticket) for ticket in result.promoted],
        "waiting": [
            {"ticket": ticket_payload(ticket), "reason": reason}
            for ticket, reason in result.waiting
        ],
    }
    if output_format == "json":
        return json.dumps(payload, indent=2, sort_keys=True)
    prefix = "DRY RUN: " if dry_run else ""
    lines = [f"{prefix}Local admission result"]
    if result.promoted:
        lines.append("Promoted:")
        lines.extend(f"- {ticket.id} | {ticket.title}" for ticket in result.promoted)
    else:
        lines.append("Promoted: none")
    if result.waiting:
        lines.append("Waiting:")
        lines.extend(
            f"- {ticket.id} | {reason}" for ticket, reason in result.waiting
        )
    return "\n".join(lines)


def render_transition(ticket: LedgerTicket, output_format: OutputFormat) -> str:
    payload = ticket_payload(ticket)
    if output_format == "json":
        return json.dumps(payload, indent=2, sort_keys=True)
    return f"{ticket.id} -> {ticket.status}"


def render_events(
    events: Sequence[Mapping[str, object]],
    output_format: OutputFormat,
) -> str:
    if output_format == "json":
        return json.dumps(list(events), indent=2, sort_keys=True)
    if not events:
        return "No ledger events."
    lines = ["Recent ledger events:"]
    for event in events:
        status = ""
        if event.get("from_status") or event.get("to_status"):
            status = f" {event.get('from_status')} -> {event.get('to_status')}"
        lines.append(
            f"- {event['created_at']} | {event['ticket_id']} | "
            f"{event['event_type']}{status} | {event['message']}"
        )
    return "\n".join(lines)


def ticket_payload(ticket: LedgerTicket) -> dict[str, object]:
    return {
        "id": ticket.id,
        "objective_id": ticket.objective_id,
        "sequence": ticket.sequence,
        "title": ticket.title,
        "status": ticket.status,
        "objective_impact": ticket.objective_impact,
        "conflict_domain": ticket.conflict_domain,
        "dependencies": list(ticket.dependencies),
        "owns": list(ticket.owns),
        "validation": list(ticket.validation),
    }


if __name__ == "__main__":
    raise SystemExit(main())
