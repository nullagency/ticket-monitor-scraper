# Ticket Monitor — Scraper

Headless scraper for World Cup 2026 resale prices. Fetches StubHub/Viagogo pages with
Playwright (US-IP runner) and POSTs parsed listings into the worker's `/ingest` and
`/ingest-calibration` endpoints.

## Scheduled runs

GitHub Actions cron fires every 15 minutes (`*/15 * * * *`). Manual trigger also available
via the **workflow_dispatch** button on the Actions tab.

## Configuration

The worker token is read from `CF_TRIGGER_TOKEN` env var. Set it as a repository secret
(Settings → Secrets and variables → Actions → New repository secret) named
`CF_TRIGGER_TOKEN`.

## What it scrapes

* **Phase A** — Mike's 4 monitored games (cc, su, s1, s6), both StubHub and Viagogo
* **Phase B** — Rotating batch of 12 calibration games from `calibration_seed.json`
  (next-7-days WC matches, used for empirical price-vs-time-to-kickoff curves)

## Local run

```bash
pip install -r requirements.txt
playwright install chromium
python local_scraper.py
```

Output goes to `/tmp/local_scraper.log` (override with `LOG_PATH` env).
