# Security And Data Handling

This repo will hold code and metadata for market-data experiments. Treat vendor
keys and raw licensed data carefully.

## Secrets

- Keep secrets in `.env`.
- Commit only `.env.example`.
- Do not print API keys in logs, reports, exceptions, or screenshots.
- Use environment variables for FMP, SEC user agent, LLM providers, and Linear.

## Vendor Data

Raw vendor responses belong in the database raw vault or local ignored data
folders. Do not commit licensed raw payloads unless Michael explicitly approves
a small fixture.

## FMP Raw-Vault Redaction Guarantee

FMP raw-vault writes must persist only sanitized request evidence:

- persisted request URLs must strip or replace API keys and other secret query
  values before storage
- persisted params must replace `apikey`, token, secret, password, and
  credential-like fields with the repository redaction marker
- logs, exceptions, reports, screenshots, and proof packets must not print
  `FMP_API_KEY`, `DATABASE_URL`, or local credentials

The response body remains the vendor response body for auditability. Silver
must not add secrets to request bodies, metadata, or headers. If a vendor error
body visibly echoes an API key or credential, stop and route the work to Safety
Review instead of persisting or publishing that secret.

## Local Files

Ignored local paths include:

- `.env`
- `.symphony/`
- `silver-agent-workspaces/`
- `data/raw/`
- generated `reports/**/*.md`

## Pull Requests

Before opening or pushing a PR, check:

```bash
git status --short
git diff --check
git check-ignore .env
```

If `.env` appears as tracked or staged, stop and fix that before doing anything
else.
