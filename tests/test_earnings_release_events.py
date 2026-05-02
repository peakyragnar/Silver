from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from silver.releases import (
    EarningsReleaseParseError,
    SecArchiveDocument,
    parse_earnings_release_exhibit,
    parse_sec_archive_index_documents,
    parse_sec_earnings_release_candidates,
    select_earnings_exhibit,
)


def test_sec_submissions_parser_filters_8k_item_202_candidates() -> None:
    payload = {
        "filings": {
            "recent": {
                "accessionNumber": [
                    "0000320193-26-000011",
                    "0000320193-26-000010",
                    "0000320193-13-000001",
                ],
                "filingDate": ["2026-04-30", "2026-04-25", "2013-12-31"],
                "reportDate": ["2026-04-30", "2026-04-25", "2013-12-31"],
                "acceptanceDateTime": [
                    "2026-04-30T16:30:41.000Z",
                    "2026-04-25T12:00:00.000Z",
                    "2013-12-31T12:00:00.000Z",
                ],
                "form": ["8-K", "10-Q", "8-K"],
                "primaryDocument": [
                    "aapl-20260430.htm",
                    "aapl-20260425.htm",
                    "aapl-20131231.htm",
                ],
                "items": ["2.02,9.01", "", "2.02,9.01"],
            }
        }
    }

    [candidate] = parse_sec_earnings_release_candidates(
        payload,
        since_date=date(2014, 1, 1),
    )

    assert candidate.accession_number == "0000320193-26-000011"
    assert candidate.filing_date == date(2026, 4, 30)
    assert candidate.report_date == date(2026, 4, 30)
    assert candidate.accepted_at == datetime(
        2026,
        4,
        30,
        20,
        30,
        41,
        tzinfo=timezone.utc,
    )
    assert candidate.primary_document == "aapl-20260430.htm"


def test_archive_index_selects_exhibit_991() -> None:
    payload = {
        "directory": {
            "item": [
                {
                    "name": "aapl-20260430.htm",
                    "size": "37639",
                    "last-modified": "2026-04-30 16:30:41",
                },
                {
                    "name": "a8-kex991q2202603282026.htm",
                    "size": "168815",
                    "last-modified": "2026-04-30 16:30:41",
                },
            ]
        }
    }

    documents = parse_sec_archive_index_documents(payload)
    exhibit = select_earnings_exhibit(
        documents,
        primary_document="aapl-20260430.htm",
    )

    assert exhibit == SecArchiveDocument(
        name="a8-kex991q2202603282026.htm",
        size=168815,
        modified_at=datetime(2026, 4, 30, 16, 30, 41, tzinfo=timezone.utc),
    )


def test_exhibit_parser_extracts_fiscal_period_identity() -> None:
    payload = b"""
    <html>
      <head><title>Apple reports second quarter results</title></head>
      <body>
        CUPERTINO, Calif.--Apple today announced financial results for its
        fiscal 2026 second quarter ended March 28, 2026. The Company posted
        quarterly revenue of $111.2 billion.
      </body>
    </html>
    """

    evidence = parse_earnings_release_exhibit(payload)

    assert evidence.fiscal_year == 2026
    assert evidence.fiscal_period == "Q2"
    assert evidence.period_end_date == date(2026, 3, 28)
    assert evidence.confidence == "high"
    assert evidence.title == "Apple reports second quarter results"
    assert "financial results" in evidence.evidence_excerpt


def test_exhibit_parser_rejects_non_earnings_release() -> None:
    with pytest.raises(EarningsReleaseParseError, match="earnings"):
        parse_earnings_release_exhibit(
            b"<html><body>Apple launches a new developer program.</body></html>"
        )
