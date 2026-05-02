"""Parse SEC 8-K Item 2.02 earnings-release evidence."""

from __future__ import annotations

import html
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from html.parser import HTMLParser
from typing import Any
from zoneinfo import ZoneInfo


ITEM_202_RE = re.compile(r"(^|[,\s])2\.02([,\s]|$)")
MONTH_NAMES = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}
QUARTER_WORDS = {
    "first": "Q1",
    "1st": "Q1",
    "second": "Q2",
    "2nd": "Q2",
    "third": "Q3",
    "3rd": "Q3",
    "fourth": "Q4",
    "4th": "Q4",
}
SEC_EDGAR_TIMEZONE = ZoneInfo("America/New_York")


class EarningsReleaseParseError(ValueError):
    """Raised when SEC release evidence cannot be parsed safely."""


@dataclass(frozen=True, slots=True)
class EarningsReleaseCandidate:
    """One SEC submissions-index row that looks like an earnings 8-K."""

    accession_number: str
    filing_date: date
    report_date: date | None
    accepted_at: datetime
    form_type: str
    item_codes: str
    primary_document: str


@dataclass(frozen=True, slots=True)
class SecArchiveDocument:
    """One SEC accession archive directory document."""

    name: str
    size: int | None
    modified_at: datetime | None


@dataclass(frozen=True, slots=True)
class EarningsReleaseEvidence:
    """Fiscal-period identity parsed from an earnings-release exhibit."""

    fiscal_year: int
    fiscal_period: str
    period_end_date: date
    confidence: str
    match_method: str
    title: str | None
    evidence_excerpt: str


def parse_sec_earnings_release_candidates(
    payload: Any,
    *,
    since_date: date,
) -> tuple[EarningsReleaseCandidate, ...]:
    """Extract 8-K Item 2.02 candidates from an SEC submissions response."""
    rows = _recent_rows(payload)
    candidates: list[EarningsReleaseCandidate] = []
    for row in rows:
        form_type = _row_str(row, "form")
        if form_type not in {"8-K", "8-K/A"}:
            continue
        item_codes = _row_str(row, "items")
        if ITEM_202_RE.search(item_codes) is None:
            continue
        filing_date = _row_date(row, "filingDate")
        if filing_date < since_date:
            continue
        candidates.append(
            EarningsReleaseCandidate(
                accession_number=_accession_number(_row_str(row, "accessionNumber")),
                filing_date=filing_date,
                report_date=_optional_row_date(row, "reportDate"),
                accepted_at=_accepted_at(_row_str(row, "acceptanceDateTime")),
                form_type=form_type,
                item_codes=item_codes,
                primary_document=_document_name(_row_str(row, "primaryDocument")),
            )
        )
    return tuple(
        sorted(candidates, key=lambda item: item.accepted_at, reverse=True)
    )


def parse_sec_archive_index_documents(payload: Any) -> tuple[SecArchiveDocument, ...]:
    """Parse an SEC archive ``index.json`` response into document rows."""
    if not isinstance(payload, Mapping):
        raise EarningsReleaseParseError("SEC archive index must be a JSON object")
    directory = payload.get("directory")
    if not isinstance(directory, Mapping):
        raise EarningsReleaseParseError("SEC archive index missing directory object")
    raw_items = directory.get("item")
    if not isinstance(raw_items, Sequence) or isinstance(
        raw_items,
        (str, bytes, bytearray),
    ):
        raise EarningsReleaseParseError("SEC archive index item must be a list")
    documents: list[SecArchiveDocument] = []
    for index, item in enumerate(raw_items):
        if not isinstance(item, Mapping):
            raise EarningsReleaseParseError(f"SEC archive item {index} must be object")
        name = _document_name(_row_str(item, "name"))
        documents.append(
            SecArchiveDocument(
                name=name,
                size=_optional_int(item.get("size")),
                modified_at=_optional_modified_at(item.get("last-modified")),
            )
        )
    return tuple(documents)


def select_earnings_exhibit(
    documents: Sequence[SecArchiveDocument],
    *,
    primary_document: str,
) -> SecArchiveDocument | None:
    """Choose the likely EX-99.1 earnings exhibit from accession documents."""
    primary = primary_document.lower()
    html_documents = [
        document
        for document in documents
        if document.name.lower().endswith((".htm", ".html"))
        and document.name.lower() != primary
    ]
    preferred = [
        document
        for document in html_documents
        if _looks_like_exhibit_991(document.name)
    ]
    if preferred:
        return sorted(preferred, key=lambda item: item.name)[0]
    return None


def parse_earnings_release_exhibit(payload: bytes) -> EarningsReleaseEvidence:
    """Parse an earnings release exhibit and extract fiscal-period identity."""
    if not isinstance(payload, (bytes, bytearray, memoryview)):
        raise EarningsReleaseParseError("release exhibit payload must be bytes")
    text = _html_text(bytes(payload))
    normalized_text = _normalize_space(text)
    if not normalized_text:
        raise EarningsReleaseParseError("release exhibit text was empty")
    lowered = normalized_text.lower()
    if not _looks_like_earnings_release(lowered):
        raise EarningsReleaseParseError("release exhibit did not look like earnings")

    period_end_date = _period_end_date(normalized_text)
    fiscal_year = _fiscal_year(normalized_text, period_end_date)
    fiscal_period = _fiscal_period(normalized_text)
    title = _title(bytes(payload))
    confidence = "high" if fiscal_period != "FY" else "medium"
    return EarningsReleaseEvidence(
        fiscal_year=fiscal_year,
        fiscal_period=fiscal_period,
        period_end_date=period_end_date,
        confidence=confidence,
        match_method="sec_8k_item_202_exhibit_period_text",
        title=title,
        evidence_excerpt=normalized_text[:500],
    )


def _recent_rows(payload: Any) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(payload, Mapping):
        raise EarningsReleaseParseError("SEC submissions response must be an object")
    filings = payload.get("filings")
    if not isinstance(filings, Mapping):
        raise EarningsReleaseParseError("SEC submissions response missing filings")
    recent = filings.get("recent")
    if not isinstance(recent, Mapping):
        raise EarningsReleaseParseError("SEC submissions response missing recent filings")

    columns = (
        "accessionNumber",
        "filingDate",
        "reportDate",
        "acceptanceDateTime",
        "form",
        "primaryDocument",
        "items",
    )
    values: dict[str, Sequence[Any]] = {}
    length: int | None = None
    for column in columns:
        raw = recent.get(column)
        if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes, bytearray)):
            raise EarningsReleaseParseError(f"SEC recent.{column} must be a list")
        if length is None:
            length = len(raw)
        elif len(raw) != length:
            raise EarningsReleaseParseError("SEC recent filing columns length mismatch")
        values[column] = raw

    rows: list[Mapping[str, Any]] = []
    for index in range(length or 0):
        rows.append({column: values[column][index] for column in columns})
    return tuple(rows)


def _html_text(payload: bytes) -> str:
    parser = _TextExtractor()
    parser.feed(payload.decode("utf-8", errors="replace"))
    parser.close()
    return html.unescape(" ".join(parser.parts))


def _title(payload: bytes) -> str | None:
    raw = payload.decode("utf-8", errors="replace")
    match = re.search(r"<title[^>]*>(.*?)</title>", raw, flags=re.I | re.S)
    if match is None:
        return None
    value = _normalize_space(html.unescape(re.sub(r"<[^>]+>", " ", match.group(1))))
    return value or None


def _normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _looks_like_exhibit_991(name: str) -> bool:
    lowered = name.lower()
    compact = lowered.replace("-", "").replace("_", "")
    return (
        "ex99" in compact
        or "exhibit99" in compact
        or "ex991" in compact
        or "exhibit991" in compact
    )


def _looks_like_earnings_release(lowered_text: str) -> bool:
    return (
        ("financial results" in lowered_text or "earnings" in lowered_text)
        and ("quarter" in lowered_text or "fiscal year" in lowered_text)
    )


def _period_end_date(text: str) -> date:
    pattern = (
        r"(?:quarter|fiscal year|year)\s+ended\s+"
        r"([A-Z][a-z]+)\s+(\d{1,2}),\s+(\d{4})"
    )
    match = re.search(pattern, text)
    if match is None:
        raise EarningsReleaseParseError("could not find reported period end date")
    month = MONTH_NAMES.get(match.group(1).lower())
    if month is None:
        raise EarningsReleaseParseError("reported period end month was unknown")
    return date(int(match.group(3)), month, int(match.group(2)))


def _fiscal_year(text: str, period_end_date: date) -> int:
    match = re.search(r"\bfiscal\s+(\d{4})\b", text, flags=re.I)
    if match is not None:
        return int(match.group(1))
    return period_end_date.year


def _fiscal_period(text: str) -> str:
    lowered = text.lower()
    for word, fiscal_period in QUARTER_WORDS.items():
        if re.search(rf"\b{re.escape(word)}\s+quarter\b", lowered):
            return fiscal_period
    if re.search(r"\bfiscal\s+year\s+ended\b", lowered):
        return "FY"
    raise EarningsReleaseParseError("could not find fiscal quarter in release text")


def _row_str(row: Mapping[str, Any], key: str) -> str:
    value = row.get(key)
    if not isinstance(value, str) or not value.strip():
        raise EarningsReleaseParseError(f"SEC row missing required {key}")
    return value.strip()


def _row_date(row: Mapping[str, Any], key: str) -> date:
    value = _row_str(row, key)
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise EarningsReleaseParseError(f"SEC row {key} must be YYYY-MM-DD") from exc


def _optional_row_date(row: Mapping[str, Any], key: str) -> date | None:
    value = row.get(key)
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise EarningsReleaseParseError(f"SEC row {key} must be YYYY-MM-DD")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise EarningsReleaseParseError(f"SEC row {key} must be YYYY-MM-DD") from exc


def _accepted_at(value: str) -> datetime:
    raw = value.strip()
    # SEC submissions timestamps are EDGAR wall-clock times. The API currently
    # renders them with a trailing Z, but the archive pages and market releases
    # align with Eastern time, not UTC.
    normalized = raw[:-1] if raw.endswith("Z") else raw
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise EarningsReleaseParseError("acceptanceDateTime must be an ISO timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=SEC_EDGAR_TIMEZONE)
    return parsed.astimezone(timezone.utc)


def _optional_modified_at(value: object) -> datetime | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise EarningsReleaseParseError("last-modified must be a timestamp string")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except ValueError as exc:
        raise EarningsReleaseParseError("last-modified must be YYYY-MM-DD HH:MM:SS") from exc
    return parsed.replace(tzinfo=timezone.utc)


def _optional_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    raise EarningsReleaseParseError("document size must be an integer")


def _accession_number(value: str) -> str:
    if re.fullmatch(r"\d{10}-\d{2}-\d{6}", value) is None:
        raise EarningsReleaseParseError("accession number has invalid format")
    return value


def _document_name(value: str) -> str:
    if "/" in value or "\\" in value or value in {".", ".."}:
        raise EarningsReleaseParseError("SEC document name must be a filename")
    return value


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() in {"script", "style"}:
            self._skip_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in {"script", "style"} and self._skip_depth:
            self._skip_depth -= 1

    def handle_data(self, data: str) -> None:
        if not self._skip_depth and data.strip():
            self.parts.append(data.strip())


def json_payload(body: bytes) -> Any:
    """Parse JSON response bytes for SEC release ingest."""
    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        raise EarningsReleaseParseError("SEC response was not valid JSON") from exc
