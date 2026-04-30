# raw-vault-failed-fmp-responses

Objective:
Raw-vault failed FMP HTTP responses so Silver has an immutable audit trail for
vendor failures that shape data coverage.

Approval Mode:
objective-doc PR.

User Value:
Michael can inspect why FMP data coverage changed or failed because non-2xx
responses, rate limits, and malformed vendor error bodies are captured before
the client raises.

Why Now:
Repository review found that `FMPClient.fetch_historical_daily_prices` raises on
non-2xx responses before writing to the raw vault, and transient failed attempts
are discarded when a later retry succeeds. That leaves no durable evidence for
vendor outages, rate limits, or malformed error bodies that affected ingest.

Done When:
- Every FMP HTTP response produced by the transport is written to the raw vault
  before parsing or raising.
- Non-2xx terminal responses are raw-vaulted before `FMPHTTPError` is raised.
- Transient failed attempts are raw-vaulted even when a later retry succeeds.
- Raw-vault metadata distinguishes attempt number, max retries, terminal versus
  retryable response, and final success/failure context.
- API keys remain redacted from persisted request URL and params.
- Focused tests prove success, terminal failure, transient-then-success, and
  transient-exhaustion audit paths.

Out Of Scope:
- No live FMP calls.
- No schema migration unless the existing raw vault cannot represent the
  evidence.
- No changes to normalized price parsing semantics.
- No changes to feature definitions, labels, backtests, or model behavior.
- No changes to the dollar-volume adjusted/raw price finding.

Guardrails:
- Never persist FMP API keys or local credentials.
- Preserve current successful FMP ingest behavior.
- Preserve raw-vault request fingerprint semantics unless contract work proves
  a change is necessary.
- Keep implementation inside FMP source-client and tests unless the contract
  proves the raw vault needs a shared helper.
- Do not introduce live or paid vendor access in tests.

ARR-62 Contract:
- Scope: every FMP HTTP response produced by the transport is raw-vault
  evidence, including successful 2xx responses, transient failed responses that
  will be retried, and terminal non-2xx responses that will raise.
- Ordering: the client must attempt the raw-vault write before parsing,
  normalization, retry sleep, success return, or raising `FMPHTTPError`.
- Required raw-vault fields: redacted request URL, redacted params, HTTP status,
  content type when available, exact response bytes, body hash, request
  fingerprint, `fetched_at`, and metadata.
- Required metadata: `audit_contract=fmp-response-audit-v1`, one-based attempt
  number, max retries, max attempts, retryable flag, terminal flag, and attempt
  outcome of `success`, `retry_scheduled`, or `terminal_failure`.
- Redaction guarantee: persisted request URLs and params must redact `apikey`
  and credential-like fields. Logs, exceptions, reports, screenshots, and proof
  packets must not print `FMP_API_KEY`, `DATABASE_URL`, or local credentials.
- No live calls: all contract and implementation validation uses mocked FMP
  transport responses unless Michael explicitly approves live vendor access.

Schema Finding:
- No schema migration is required to persist distinct failed-response bodies,
  terminal non-2xx responses before raise, transient-before-success responses,
  redacted request evidence, or per-row attempt metadata because
  `silver.raw_objects` already has `request_url`, `params`, `http_status`,
  body storage, hashes, `fetched_at`, and `metadata`.
- The existing raw-vault uniqueness key is `(vendor, endpoint, params_hash,
  raw_hash)`. Byte-identical retries for the same request can therefore resolve
  to one existing raw object. The client must still attempt a raw-vault write
  for every transport-produced response, but strict per-attempt row cardinality
  for byte-identical retry bodies needs a follow-up schema ticket, such as an
  additive attempt-event table keyed to `raw_objects.id`.
- ARR-62 does not authorize a migration. Downstream implementation must not
  claim strict byte-identical retry cardinality unless that schema gap is closed
  by a migration-owner ticket.

Project Adapter:
Silver project adapter: FMP ingestion, raw vault, secret redaction, and local
mocked tests.

Runner Adapter:
Linear/Symphony.

Expected Tickets:
- Define failed FMP raw-vault audit contract
  Ticket Role: contract
  Dependency Group: fmp-raw-vault
  Contracts Touched:
  - fmp-response-audit
  - raw-vault-ingest
  Risk Class: low
  Purpose: Define the failed-response raw-vault policy before client behavior
  changes.
  Expected Impact On Objective: Downstream implementation has one approved
  contract for which FMP responses are persisted, what metadata is required, and
  what remains out of scope.
  Technical Summary: Document that every transport-produced FMP HTTP response,
  including transient failed attempts and terminal non-2xx responses, must be
  written to the existing raw vault with redacted URL/params and attempt
  metadata before parsing or raising.
  Owns:
  - `docs/ARCHITECTURE.md`
  - `docs/SECURITY.md`
  - `docs/PHASE1_RUNBOOK.md`
  - `docs/objectives/active/raw-vault-failed-fmp-responses.md`
  Do Not Touch:
  - `db/migrations/`
  - `src/silver/features/`
  - `src/silver/backtest/`
  - `scripts/run_falsifier.py`
  Dependencies:
  - none
  Conflict Zones:
  - `docs/ARCHITECTURE.md`
  - `docs/SECURITY.md`
  - `docs/PHASE1_RUNBOOK.md`
  Validation:
  - `git diff --check`
  - `python scripts/planning_steward.py --check`
  Proof Packet:
  - failed-response raw-vault policy
  - no-schema-change or schema-change justification
  - redaction guarantee
- Persist failed FMP HTTP attempts
  Ticket Role: implementation
  Dependency Group: fmp-raw-vault
  Contracts Touched:
  - fmp-response-audit
  - raw-vault-ingest
  Risk Class: low
  Purpose: Write all FMP HTTP responses to the raw vault before retry,
  returning success, or raising.
  Expected Impact On Objective: Vendor failures and retry history become
  inspectable audit artifacts instead of disappearing when the client raises or
  later succeeds.
  Technical Summary: Refactor `FMPClient.fetch_historical_daily_prices` and the
  retry helper so each transport-produced `FMPTransportResponse` is persisted
  once with attempt metadata, terminal/retryable context, redacted request
  evidence, and the existing raw-vault result.
  Owns:
  - `src/silver/sources/fmp/client.py`
  - `tests/test_fmp_client.py`
  Do Not Touch:
  - `db/migrations/`
  - `src/silver/features/`
  - `src/silver/backtest/`
  - `src/silver/reports/`
  Dependencies:
  - Define failed FMP raw-vault audit contract
  Conflict Zones:
  - `src/silver/sources/fmp/client.py`
  - `tests/test_fmp_client.py`
  Validation:
  - `python -m pytest tests/test_fmp_client.py`
  - `python -m pytest tests/test_ingest_fmp_prices.py`
  Proof Packet:
  - test proving terminal non-2xx response is raw-vaulted before raising
  - test proving transient failed attempt is raw-vaulted before retry success
  - test proving transient exhaustion raw-vaults every response
  - redaction assertion for failed responses
- Validate FMP raw-vault audit trail
  Ticket Role: validation
  Dependency Group: fmp-raw-vault
  Contracts Touched:
  - fmp-response-audit
  - raw-vault-ingest
  Risk Class: low
  Purpose: Prove the full FMP raw-vault failure audit path without live vendor
  calls.
  Expected Impact On Objective: The Objective closes with focused and broad
  evidence that failed FMP responses are persisted and existing ingest behavior
  remains stable.
  Technical Summary: Run focused FMP client and ingest tests plus broader repo
  validation; record the no-live-call proof and any skipped live FMP command
  reason.
  Owns:
  - `docs/objectives/active/raw-vault-failed-fmp-responses.md`
  - `tests/test_fmp_client.py`
  - `tests/test_ingest_fmp_prices.py`
  Do Not Touch:
  - `db/migrations/`
  - `src/silver/features/`
  - `src/silver/backtest/`
  Dependencies:
  - Persist failed FMP HTTP attempts
  Conflict Zones:
  - `tests/test_fmp_client.py`
  - `tests/test_ingest_fmp_prices.py`
  Validation:
  - `python -m pytest tests/test_fmp_client.py tests/test_ingest_fmp_prices.py`
  - `python -m pytest`
  - `ruff check .`
  - `git diff --check`
  Proof Packet:
  - focused FMP audit-path test output
  - full validation output
  - explicit no-live-FMP-calls statement

Validation:
- `git diff --check`
- `python scripts/planning_steward.py --check`
- `python -m pytest tests/test_fmp_client.py tests/test_ingest_fmp_prices.py`
- `python -m pytest`
- `ruff check .`

Conflict Zones:
- `src/silver/sources/fmp/client.py`
- `tests/test_fmp_client.py`
- `tests/test_ingest_fmp_prices.py`
- `docs/ARCHITECTURE.md`
- `docs/SECURITY.md`
- `docs/PHASE1_RUNBOOK.md`
