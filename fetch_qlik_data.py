"""
Qlik data fetcher for the Adspend Decision Dashboard.

Pulls Premium User Data and Adspend Data from the GROWTH_HIGH_LEVEL Upgrade app
and writes them as XLSX files matching the column format the dashboard expects.

Usage:
    python fetch_qlik_data.py                # writes to ./data/
    python fetch_qlik_data.py --out ./data   # custom output directory
    python fetch_qlik_data.py --json         # outputs JSON (for serverless use)

Environment variables (or .env file):
    QLIK_TENANT   e.g. https://yourname.eu.qlikcloud.com
    QLIK_API_KEY  Qlik Cloud API key (Bearer token)
    QLIK_APP_ID   App UUID (defaults to GROWTH_HIGH_LEVEL Upgrade)
    START_DATE    ISO date for earliest data (default: 2025-01-01)
"""

import asyncio, json, os, sys, csv, io, datetime, argparse, pathlib, math
import urllib.request, urllib.parse

try:
    import websockets
except ImportError:
    sys.exit("Missing dependency: pip install websockets")

try:
    import openpyxl
    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
def _env(key, default=None):
    val = os.environ.get(key)  # None when not set
    if val is None:
        env_path = pathlib.Path(__file__).parent / ".env"
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                if line.startswith(key + "="):
                    val = line.split("=", 1)[1].strip()
                    break
    return val if val is not None else default

TENANT  = _env("QLIK_TENANT", "https://2mfrj5yb8kxk6yq.eu.qlikcloud.com").rstrip("/")
API_KEY = _env("QLIK_API_KEY", "")
APP_ID  = _env("QLIK_APP_ID",  "67689fd6-5c3f-49e9-ad1d-e2c6854d6a27")
# Start date for data pull (Jan 1 of last year by default keeps ~2 seasons)
_start_str = _env("START_DATE", "2025-01-01")
START_DATE  = datetime.date.fromisoformat(_start_str)

def date_to_serial(d: datetime.date) -> int:
    """Convert date to Excel / Qlik calendar_date_num serial integer."""
    return (d - datetime.date(1899, 12, 30)).days

START_SERIAL = date_to_serial(START_DATE)
TODAY_SERIAL = date_to_serial(datetime.date.today())

# ---------------------------------------------------------------------------
# Qlik Engine WebSocket helpers
# ---------------------------------------------------------------------------
_msg_id = 0
def _msg(method, params=None, handle=-1):
    global _msg_id
    _msg_id += 1
    return json.dumps({"jsonrpc":"2.0","id":_msg_id,"method":method,
                       "handle":handle,"params":params or []})

async def _recv(ws, expected_id, timeout=120):
    while True:
        raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
        data = json.loads(raw)
        if data.get("id") == expected_id:
            return data

async def _call(ws, method, params=None, handle=-1, timeout=120):
    global _msg_id
    await ws.send(_msg(method, params, handle))
    return await _recv(ws, _msg_id, timeout)

async def _fetch_hypercube(ws, doc_handle, dims, measures, page_size=None,
                          suppress_zero=True):
    """Create a session table object and page through all rows."""
    _ncols = len(dims) + len(measures)
    obj_def = {
        "qInfo": {"qType": "Table"},
        "qHyperCubeDef": {
            "qMode": "S",
            "qSuppressZero": suppress_zero,
            "qSuppressMissing": True,
            "qDimensions": [{"qDef": {"qFieldDefs": [d]}} for d in dims],
            "qMeasures":   [{"qDef": {"qDef": m}} for m in measures],
            "qInitialDataFetch": [{"qTop":0,"qLeft":0,"qHeight":1,"qWidth":_ncols}]
        }
    }
    resp = await _call(ws, "CreateSessionObject", [obj_def], handle=doc_handle)
    if "error" in resp:
        raise RuntimeError(f"CreateSessionObject error: {resp['error']}")
    obj_handle = resp["result"]["qReturn"]["qHandle"]

    layout = (await _call(ws, "GetLayout", [], handle=obj_handle))["result"]["qLayout"]
    total = layout["qHyperCube"]["qSize"]["qcy"]
    print(f"  Total rows: {total}", flush=True)

    # Qlik limits hypercube pages to ~10,000 cells (rows × columns)
    effective_page = min(page_size or 9999, max(1, 9000 // _ncols))

    n_dims = len(dims)
    all_rows = []
    offset = 0
    while offset < total:
        page_req = [{"qTop":offset,"qLeft":0,"qHeight":min(effective_page,total-offset),"qWidth":_ncols}]
        resp = await _call(ws, "GetHyperCubeData", ["/qHyperCubeDef", page_req], handle=obj_handle)
        if "error" in resp:
            raise RuntimeError(f"GetHyperCubeData error: {resp['error']}")
        matrix = resp["result"]["qDataPages"][0]["qMatrix"]
        for row in matrix:
            cells = []
            for i, c in enumerate(row):
                if i >= n_dims:
                    # measure: qType is absent — use qNum directly, guard against NaN
                    num = c.get("qNum", 0)
                    cells.append(0.0 if (num is None or (isinstance(num, float) and math.isnan(num))) else num)
                else:
                    cells.append(c.get("qText", ""))
            all_rows.append(cells)
        offset += len(matrix)
        print(f"  Fetched {offset}/{total} rows...", end="\r", flush=True)
    print()
    return all_rows

# ---------------------------------------------------------------------------
# Main fetch logic
# ---------------------------------------------------------------------------
async def fetch_all():
    ws_url = TENANT.replace("https://", "wss://").replace("http://", "ws://") + f"/app/{APP_ID}"
    headers = {"Authorization": f"Bearer {API_KEY}"}

    print(f"Connecting to Qlik Engine: {ws_url}", flush=True)
    async with websockets.connect(ws_url, additional_headers=headers,
                                  open_timeout=30, ping_timeout=120,
                                  max_size=50_000_000) as ws:
        await asyncio.wait_for(ws.recv(), timeout=15)  # OnConnected
        print("Connected. Opening app...", flush=True)
        resp = await _call(ws, "OpenDoc", [APP_ID], timeout=90)
        if "error" in resp:
            raise RuntimeError(f"OpenDoc failed: {resp['error']}")
        doc_handle = resp["result"]["qReturn"]["qHandle"]
        print(f"App opened (handle={doc_handle})", flush=True)

        # ----------------------------------------------------------------
        # Query 1: Premium User Data
        # Dims: country_lt, webrole_id_lt, payment_date (DD/MM/YYYY string)
        # Measures: FP, Returning, Recurring, Count(new users)
        # Date filter applied in Python after fetch (filter payment_date >= START_DATE)
        # ----------------------------------------------------------------
        print("\nFetching Premium User Data...", flush=True)
        prem_dims = ["country_lt", "webrole_id_lt", "payment_date"]
        prem_meas = [
            "Sum({<order_type2={'first'}>} amount)",
            "Sum({<order_type2={'returning'}>} amount)",
            "Sum({<order_type2={'recurring'}>} amount)",
            "Count(DISTINCT {<order_type2={'first'}>} webuser_order)",
        ]
        prem_rows = await _fetch_hypercube(ws, doc_handle, prem_dims, prem_meas,
                                           suppress_zero=True)

        # ----------------------------------------------------------------
        # Query 2: Adspend Data
        # Dims: country_lt, webrole_id_lt, Date (YYYY-MM-DD string), channel
        # Measures: Sum(Cost)
        # Date filter applied in Python after fetch
        # ----------------------------------------------------------------
        print("\nFetching Adspend Data...", flush=True)
        adspend_dims = ["country_lt", "webrole_id_lt", "Date", "channel"]
        adspend_meas = ["Sum(Cost)"]
        ad_rows = await _fetch_hypercube(ws, doc_handle, adspend_dims, adspend_meas,
                                         suppress_zero=True)

    # Filter to START_DATE in Python (avoids complex Qlik set analysis on date dimensions)
    def _parse_prem_date(s):
        # Qlik payment_date returns DD/MM/YYYY (European format)
        for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y"):
            try:
                return datetime.datetime.strptime(s.strip(), fmt)
            except ValueError:
                pass
        return None

    def _parse_ad_date(s):
        # Qlik Date returns YYYY-MM-DD
        for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
            try:
                return datetime.datetime.strptime(s.strip(), fmt)
            except ValueError:
                pass
        return None

    start_dt = datetime.datetime.combine(START_DATE, datetime.time.min)

    filtered_prem = []
    for r in prem_rows:
        if not r[0] or r[0] == "-":
            continue
        dt = _parse_prem_date(r[2])
        if dt and dt >= start_dt:
            filtered_prem.append(r)

    filtered_ad = []
    for r in ad_rows:
        if not r[0] or r[0] == "-":
            continue
        dt = _parse_ad_date(r[2])
        if dt and dt >= start_dt:
            filtered_ad.append(r)

    print(f"  After date filter (>= {START_DATE}): {len(filtered_prem)} premium, "
          f"{len(filtered_ad)} adspend rows")
    return filtered_prem, filtered_ad


def _parse_date_any(s):
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.datetime.strptime(s.strip(), fmt)
        except (ValueError, AttributeError):
            pass
    return None


def build_premium_xlsx(rows):
    """Build XLSX bytes with the exact column names the dashboard expects."""
    if not HAS_OPENPYXL:
        return None
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Sheet1"
    headers = ["country_lt","webrole id","calendar date",
               "(€) First Payments","(€) Returning","(€) Recurring",
               "Total No. NEW Premium Users"]
    ws.append(headers)
    for r in rows:
        country, role, date_str, fp, ret, rec, users = r[0], r[1], r[2], r[3], r[4], r[5], r[6]
        dt = _parse_date_any(date_str) or date_str
        ws.append([country, role, dt,
                   float(fp or 0), float(ret or 0), float(rec or 0), int(users or 0)])
    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()

def build_adspend_xlsx(rows):
    if not HAS_OPENPYXL:
        return None
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Sheet1"
    headers = ["country_lt","webrole id","calendar date","Adspend","adspend_channel"]
    ws.append(headers)
    for r in rows:
        # dims: country_lt(0), webrole_id_lt(1), Date(2), channel(3); measure: Cost(4)
        country, role, date_str, channel, cost = r[0], r[1], r[2], r[3], r[4]
        dt = _parse_date_any(date_str) or date_str
        ws.append([country, role, dt, float(cost or 0), channel])
    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()

def rows_to_json(prem_rows, ad_rows):
    """Return dict with both datasets as JSON-serialisable lists."""
    prem_headers = ["country_lt","webrole id","calendar date",
                    "(€) First Payments","(€) Returning","(€) Recurring",
                    "Total No. NEW Premium Users"]
    ad_headers = ["country_lt","webrole id","calendar date","Adspend","adspend_channel"]
    return {
        "generated": datetime.datetime.now(datetime.UTC).isoformat().replace("+00:00", "Z"),
        "startDate": START_DATE.isoformat(),
        "premiumData": {
            "headers": prem_headers,
            "rows": [[r[0],r[1],r[2],
                      float(r[3] or 0), float(r[4] or 0),
                      float(r[5] or 0), int(r[6] or 0)] for r in prem_rows]
        },
        "adspendData": {
            "headers": ad_headers,
            # dims: country(0), role(1), Date(2), channel(3); measure: Cost(4)
            "rows": [[r[0], r[1], r[2], float(r[4] or 0), r[3]] for r in ad_rows]
        }
    }

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    global _msg_id
    _msg_id = 0

    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="data", help="Output directory for XLSX files")
    parser.add_argument("--json", action="store_true", help="Write JSON instead of XLSX")
    args = parser.parse_args()

    if not API_KEY:
        sys.exit("QLIK_API_KEY not set — add it to .env or environment variables")

    prem_rows, ad_rows = asyncio.run(fetch_all())
    print(f"\nFetched {len(prem_rows)} premium rows, {len(ad_rows)} adspend rows")

    out_dir = pathlib.Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.json:
        payload = rows_to_json(prem_rows, ad_rows)
        out_path = out_dir / "qlik_data.json"
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
        print(f"JSON written to {out_path}")
    else:
        if not HAS_OPENPYXL:
            sys.exit("openpyxl required for XLSX output: pip install openpyxl")
        prem_bytes = build_premium_xlsx(prem_rows)
        ad_bytes   = build_adspend_xlsx(ad_rows)
        prem_path = out_dir / f"premium_data_{datetime.date.today()}.xlsx"
        ad_path   = out_dir / f"adspend_data_{datetime.date.today()}.xlsx"
        prem_path.write_bytes(prem_bytes)
        ad_path.write_bytes(ad_bytes)
        print(f"Premium data → {prem_path}")
        print(f"Adspend data → {ad_path}")

        # Also write JSON for dashboard auto-load
        payload = rows_to_json(prem_rows, ad_rows)
        json_path = out_dir / "qlik_data.json"
        json_path.write_text(json.dumps(payload, ensure_ascii=False))
        print(f"JSON cache   → {json_path}")

if __name__ == "__main__":
    main()
