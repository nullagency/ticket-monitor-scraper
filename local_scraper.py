#!/usr/bin/env python3
"""
Local US-IP scraper. Fetches StubHub/Viagogo for all monitored games, parses with the same
regex the worker uses, POSTs the batched results to the worker's /ingest endpoint.

Runs via launchd / cron every ~10-15 minutes. This is the production data path while the
worker's CF-egress fetches return geo-degraded HTML.

Exits 0 on success. Logs to /tmp/local_scraper.log for launchd debugging.
"""
import re, json, time, subprocess, datetime, sys, traceback, tempfile, os

# Token from env (set as a GitHub secret for the Actions workflow, defaults to the known value
# for local launchd runs so the Mac scheduler still works without configuration change).
TOKEN = os.environ.get("CF_TRIGGER_TOKEN", "mike-trigger-2026")
BASE = "https://worldcup.nullagency.io"
INGEST_URL = f"{BASE}/ingest?t={TOKEN}"
CALIB_INGEST_URL = f"{BASE}/ingest-calibration?t={TOKEN}"
# Resolve seed and rotate-state paths relative to this script so it works from anywhere
# (local launchd cwd, GitHub Actions checkout dir, etc.)
_HERE = os.path.dirname(os.path.abspath(__file__))
CALIB_SEED = os.path.join(_HERE, "calibration_seed.json")
CALIB_ROTATE_STATE = os.environ.get("CALIB_ROTATE_STATE", "/tmp/local_scraper_rotate.txt")
CALIB_PER_RUN = int(os.environ.get("CALIB_PER_RUN", "12"))
LOG_PATH = os.environ.get("LOG_PATH", "/tmp/local_scraper.log")

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

# Match SOURCES in worker.js — only the parsers that are known good (drop Gametime)
SOURCES = [
    {"src": "StubHub", "game": "cc", "url": "https://www.stubhub.com/world-cup-zapopan-tickets-6-23-2026/event/153033471/?quantity=3"},
    {"src": "StubHub", "game": "su", "url": "https://www.stubhub.com/world-cup-zapopan-tickets-6-26-2026/event/156535642/?quantity=3"},
    {"src": "StubHub", "game": "s1", "url": "https://www.stubhub.com/world-cup-seattle-tickets-7-1-2026/event/153020573/?quantity=2"},
    {"src": "StubHub", "game": "s6", "url": "https://www.stubhub.com/world-cup-seattle-tickets-7-6-2026/event/153020574/?quantity=2"},
    {"src": "Viagogo", "game": "cc", "url": "https://www.viagogo.com/Sports-Tickets/Soccer/Soccer-Tournament/World-Cup-Tickets/E-153033471?quantity=3&lc=USD&clientCountry=US"},
    {"src": "Viagogo", "game": "su", "url": "https://www.viagogo.com/Sports-Tickets/Soccer/Soccer-Tournament/World-Cup-Tickets/E-156535642?quantity=3&lc=USD&clientCountry=US"},
    {"src": "Viagogo", "game": "s1", "url": "https://www.viagogo.com/Sports-Tickets/Soccer/Soccer-Tournament/World-Cup-Tickets/E-153020573?quantity=2&lc=USD&clientCountry=US"},
    {"src": "Viagogo", "game": "s6", "url": "https://www.viagogo.com/Sports-Tickets/Soccer/Soccer-Tournament/World-Cup-Tickets/E-153020574?quantity=2&lc=USD&clientCountry=US"},
]

PACK_QTY = {"cc": 3, "su": 3, "s1": 2, "s6": 2}

# NEW format (2026-06-15): StubHub/Viagogo migrated from embedded sectionPopupData JSON to
# rendered listing cards with data-* attributes. Each card:
#   data-listing-id="..."  data-feature-id="<section>_<sub>"  data-is-sold="0"  data-price="$N"
# Since the URL carries ?quantity=N, every visible listing already satisfies the pack size —
# the quantity gate happens server-side. ticketCount per-listing is no longer exposed, so we
# synthesize ticketCount = packQty to keep the worker's buyability tripwire passing.
LISTING_DATA_RE = re.compile(
    r'data-listing-id="(\d+)"\s+data-feature-id="([^"]*)"\s+data-is-sold="(\d)"\s+data-price="\$([\d,]+(?:\.\d{2})?)"'
)


def log(msg):
    line = f"[{datetime.datetime.now().isoformat(timespec='seconds')}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_PATH, "a") as f: f.write(line + "\n")
    except Exception: pass


# Playwright session reused across all source fetches in one run — much faster than
# launching a browser per URL, and StubHub/Viagogo now serve JS-challenge bot pages to
# plain curl/urllib, so a real browser is mandatory.
_PW = {"ctx": None, "browser": None, "pw": None}

def _ensure_browser():
    if _PW["ctx"] is None:
        from playwright.sync_api import sync_playwright
        _PW["pw"] = sync_playwright().start()
        _PW["browser"] = _PW["pw"].chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        _PW["ctx"] = _PW["browser"].new_context(
            user_agent=UA, viewport={"width":1440,"height":900}, locale="en-US",
            timezone_id="America/Los_Angeles",
            extra_http_headers={"Accept-Language":"en-US,en;q=0.9"},
        )
        # Apply stealth patches to every new page (hides webdriver, fixes plugins.length,
        # chrome.runtime, permissions API mismatches that bot detection inspects).
        try:
            from playwright_stealth import Stealth
            _PW["stealth"] = Stealth()
        except Exception:
            _PW["stealth"] = None
    return _PW["ctx"]

def _close_browser():
    try:
        if _PW["ctx"]: _PW["ctx"].close()
        if _PW["browser"]: _PW["browser"].close()
        if _PW["pw"]: _PW["pw"].stop()
    except Exception: pass

def fetch_html(url, timeout=45):
    ctx = _ensure_browser()
    page = ctx.new_page()
    # Apply stealth patches if available — must run BEFORE goto so they're in place when the
    # page loads its bot-detection script.
    stealth = _PW.get("stealth")
    if stealth is not None:
        try: stealth.apply_stealth_sync(page)
        except Exception: pass
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=timeout*1000)
        # Wait until the popup data is actually present — sites hydrate at very different
        # speeds depending on which event page. Accept >=1 rawMinPrice match (any popup
        # data at all); fall through silently if it never arrives within 30s.
        try:
            # NEW format: wait for at least 3 listing-card data-price attributes to render.
            # (Old format checked for sectionPopupData/rawMinPrice — both retired by the site.)
            page.wait_for_function(
                "() => document.querySelectorAll('[data-listing-id][data-price]').length >= 3",
                timeout=30000,
            )
            page.wait_for_timeout(500)
        except Exception:
            pass
        return page.content()
    finally:
        page.close()


def parse_aria(html, game):
    """Parse the NEW listing-card format. Each visible card is server-side filtered to satisfy
    the URL's ?quantity=N, so all listings are pack-buyable by construction. We synthesize
    ticketCount = packQty so the worker's downstream buyability gate (which checks ticketCount
    >= packQty) passes for every entry."""
    pack_qty = PACK_QTY.get(game, 2)
    out = []
    seen_ids = set()
    for m in LISTING_DATA_RE.finditer(html):
        listing_id, feat, sold, price_str = m.group(1), m.group(2), m.group(3), m.group(4)
        if sold != "0": continue
        if listing_id in seen_ids: continue
        seen_ids.add(listing_id)
        pp = float(price_str.replace(",", ""))
        if pp < 50 or pp > 25000: continue
        # feature_id is "<section>_<sub>" — keep the first part as the category for readability
        cat = f"section {feat.split('_')[0]}" if feat else f"listing {listing_id}"
        out.append({
            "category": cat,
            "row": "",
            "ppFrom": pp,
            "ticketCount": pack_qty,   # synthesized — URL quantity filter guarantees pack-buyable
            "listingId": listing_id,
        })
    return sorted(out, key=lambda x: x["ppFrom"])[:60]


def scrape_one(s):
    try:
        html = fetch_html(s["url"])
        mins = parse_aria(html, s["game"])
        return {"src": s["src"], "game": s["game"], "url": s["url"], "status": 200, "mins": mins}
    except Exception as e:
        log(f"  FETCH FAIL {s['src']}/{s['game']}: {e}")
        return {"src": s["src"], "game": s["game"], "url": s["url"], "status": -1, "mins": [], "error": str(e)[:200]}


def ingest(results):
    # Curl for the POST too — Cloudflare blocks default Python urllib UA at the edge before
    # the request ever reaches the worker.
    payload = json.dumps({"results": results}).encode()
    with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as f:
        f.write(payload); tmp = f.name
    try:
        r = subprocess.run([
            "curl", "-sS", "-m", "45",
            "-X", "POST",
            "-A", UA,
            "-H", "Content-Type: application/json",
            "--data-binary", f"@{tmp}",
            "-w", "\n__HTTP__:%{http_code}",
            INGEST_URL,
        ], capture_output=True, timeout=50)
    finally:
        try: os.unlink(tmp)
        except Exception: pass
    out = r.stdout.decode("utf-8", "replace")
    m = re.search(r"__HTTP__:(\d+)$", out)
    code = int(m.group(1)) if m else 0
    body = out[:m.start()] if m else out
    return code, body


def scrape_calibration():
    """Fetch the next batch of calibration games (round-robin from seed list). Returns
    list of polls suitable for /ingest-calibration."""
    try:
        with open(CALIB_SEED) as f: seeds = json.load(f)
    except FileNotFoundError:
        log("  calibration_seed.json missing; skip")
        return []
    if not seeds: return []
    # Round-robin: read offset, fetch next CALIB_PER_RUN, advance
    try:
        with open(CALIB_ROTATE_STATE) as f: offset = int(f.read().strip())
    except Exception: offset = 0
    today = datetime.date.today()
    batch = []
    polls = []
    # Refresh daysOut against today (seed file was built at discover time)
    for s in seeds:
        try: d = datetime.date.fromisoformat(s["date"])
        except: continue
        s["daysOut"] = (d - today).days
    # Drop games that already happened
    active = [s for s in seeds if s["daysOut"] >= 0]
    if not active: return []
    # Pick batch starting at offset, wrap around
    for i in range(min(CALIB_PER_RUN, len(active))):
        batch.append(active[(offset + i) % len(active)])
    new_offset = (offset + CALIB_PER_RUN) % len(active)
    with open(CALIB_ROTATE_STATE, "w") as f: f.write(str(new_offset))
    log(f"  calibration batch: {len(batch)} games (offset {offset} -> {new_offset})")
    for s in batch:
        eid = s["eventId"]
        # StubHub only — Viagogo shares the same inventory DB. quantity=2 is the most permissive
        # pack size for unknown game pack preferences.
        url = f"https://www.stubhub.com/world-cup-{s['city']}-tickets-{int(s['date'].split('-')[1])}-{int(s['date'].split('-')[2])}-2026/event/{eid}/?quantity=2"
        try:
            html = fetch_html(url, timeout=20)
            mins = parse_aria(html, "s1")  # use 2-pack rule (any game we don't own)
            if mins:
                m0 = mins[0]
                polls.append({
                    "eventId": eid, "city": s["city"], "date": s["date"], "daysOut": s["daysOut"],
                    "src": "StubHub", "ppFrom": m0["ppFrom"], "ticketCount": m0.get("ticketCount"),
                })
            log(f"    T-{s['daysOut']}d {s['city']:18} id={eid}: mins={len(mins)} cheapest=${mins[0]['ppFrom'] if mins else None}")
        except Exception as e:
            log(f"    T-{s['daysOut']}d {s['city']:18} id={eid}: FAIL {str(e)[:80]}")
    return polls


def ingest_calibration(polls):
    if not polls: return 0, "no polls"
    payload = json.dumps({"polls": polls}).encode()
    with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as f:
        f.write(payload); tmp = f.name
    try:
        r = subprocess.run([
            "curl", "-sS", "-m", "45", "-X", "POST", "-A", UA,
            "-H", "Content-Type: application/json", "--data-binary", f"@{tmp}",
            "-w", "\n__HTTP__:%{http_code}", CALIB_INGEST_URL,
        ], capture_output=True, timeout=50)
    finally:
        try: os.unlink(tmp)
        except: pass
    out = r.stdout.decode("utf-8", "replace")
    m = re.search(r"__HTTP__:(\d+)$", out)
    code = int(m.group(1)) if m else 0
    body = out[:m.start()] if m else out
    return code, body


def main():
    log("scrape start")
    # Phase A: Mike's 4 games (always all 8 sources)
    results = []
    for s in SOURCES:
        r = scrape_one(s)
        results.append(r)
        cheap = (r["mins"][0]["ppFrom"] if r["mins"] else None)
        qty   = (r["mins"][0].get("ticketCount") if r["mins"] else None)
        log(f"  {s['src']:8} {s['game']}: status={r['status']} mins={len(r['mins'])} cheapest=${cheap} qty={qty}")
        time.sleep(0.5)
    status, body = ingest(results)
    log(f"INGEST HTTP {status}: {body[:200]}")
    # Phase B: calibration batch (rotates through 30 next-7d games over multiple runs)
    log("calibration batch start")
    polls = scrape_calibration()
    cstatus, cbody = ingest_calibration(polls)
    log(f"CALIB INGEST HTTP {cstatus}: {cbody[:200]}")
    log("scrape done")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        log("FATAL " + traceback.format_exc())
        sys.exit(1)
    finally:
        _close_browser()
