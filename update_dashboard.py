# ════════════════════════════════════════════════════════════
# MARKET BASECAMP — Data Pipeline  (update_dashboard.py)
# ════════════════════════════════════════════════════════════

import os, sys, requests, time
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, date
import pytz

IST      = pytz.timezone("Asia/Kolkata")
NOW_IST  = datetime.now(IST)
TODAY    = date.today().isoformat()
HOUR_IST = NOW_IST.hour

# ── NSE trading-day gate ─────────────────────────────────────
# Markets are closed on weekends and exchange-declared holidays.
# On any non-trading day the pipeline must NOT write dated rows
# (otherwise weekend/holiday runs duplicate Friday's data under a
# new date). Source: NSE equity-segment holiday calendar.
NSE_HOLIDAYS_2026 = {
    "2026-01-15",  # Municipal Corp Election (Maharashtra)
    "2026-01-26",  # Republic Day
    "2026-03-03",  # Holi
    "2026-03-26",  # Shri Ram Navami
    "2026-03-31",  # Shri Mahavir Jayanti
    "2026-04-03",  # Good Friday
    "2026-04-14",  # Dr. Ambedkar Jayanti
    "2026-05-01",  # Maharashtra Day
    "2026-05-28",  # Bakri Id
    "2026-06-26",  # Muharram
    "2026-09-14",  # Ganesh Chaturthi
    "2026-10-02",  # Mahatma Gandhi Jayanti
    "2026-10-20",  # Dussehra
    "2026-11-10",  # Diwali-Balipratipada
    "2026-11-24",  # Guru Nanak Jayanti
    "2026-12-25",  # Christmas
}
# Note: a special Muhurat session runs on Sun 2026-11-08, but that's an
# exception we deliberately ignore (the pipeline treats it as a normal Sunday).

def is_trading_day(d=None):
    """True only on Mon–Fri that are not NSE holidays."""
    d = d or NOW_IST.date()
    if d.weekday() >= 5:                 # 5=Sat, 6=Sun
        return False
    if d.isoformat() in NSE_HOLIDAYS_2026:
        return False
    return True

from supabase import create_client
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
sb = create_client(SUPABASE_URL, SUPABASE_KEY)

# Admin identity (model-portfolio owner). Pipeline runs as service-role and reads
# the public.profiles table to resolve the admin's user_id once, cached.
ADMIN_EMAIL = os.environ.get("ADMIN_EMAIL", "traderhikes@gmail.com")
_ADMIN_UID = None
def get_admin_uid():
    global _ADMIN_UID
    if _ADMIN_UID is None:
        try:
            r = sb.table("profiles").select("user_id").eq("email", ADMIN_EMAIL).limit(1).execute().data
            _ADMIN_UID = r[0]["user_id"] if r else None
        except Exception as e:
            print(f"   ⚠️  admin uid lookup: {e}")
    return _ADMIN_UID

# RUN_MODE is normally passed explicitly by the workflow (per cron slot).
# This hour-based fallback only applies if RUN_MODE env is empty — kept in
# sync with the workflow schedule so a manual/bare run still picks sanely.
#   intraday : 9–14 IST (light CMP+index touch-ups during market hours)
#   fii_dii  : 19–20 IST (evening institutional-flow pull)
#   full_eod : everything else (incl. the 15–16 IST close runs)
if   9 <= HOUR_IST <= 14:     RUN_MODE = "intraday"
elif HOUR_IST in [19, 20]:    RUN_MODE = "fii_dii"
else:                          RUN_MODE = "full_eod"
RUN_MODE = os.environ.get("RUN_MODE", RUN_MODE) or RUN_MODE

print(f"{'='*56}\n  MARKET BASECAMP [{RUN_MODE.upper()}]")
print(f"  {NOW_IST.strftime('%d %b %Y · %I:%M %p IST')}\n{'='*56}\n")


# ════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════

# Known NSE symbol renames — applied transparently before any Yahoo fetch.
# Add new entries here as NSE renames companies (the bare symbol, no .NS).
SYMBOL_RENAMES = {
    "GMRINFRA":     "GMRAIRPORT",    # renamed to GMR Airports
    "AMBUJACEMENT": "AMBUJACEM",     # correct NSE symbol
}

def _remap(ticker_str):
    """Apply known renames; preserves any .NS / .BO suffix and index (^) tickers."""
    if not ticker_str:
        return ticker_str
    # Index tickers (start with ^) and suffixed forms pass through the map by base.
    base = ticker_str
    suffix = ""
    for sfx in (".NS", ".BO"):
        if ticker_str.endswith(sfx):
            base = ticker_str[: -len(sfx)]; suffix = sfx; break
    return SYMBOL_RENAMES.get(base, base) + suffix

def get_close_series(ticker_str, period="5d"):
    """Download a single ticker → clean 1-D Close Series."""
    ticker_str = _remap(ticker_str)
    data = yf.download(ticker_str, period=period, progress=False, auto_adjust=True)
    if data is None or data.empty:
        return pd.Series(dtype=float)
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.droplevel(1)
    close = data["Close"]
    if isinstance(close, pd.DataFrame):
        close = close.iloc[:, 0]
    return close.dropna()

def compute_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


# ════════════════════════════════════════════════════════════
# NIFTY 500 SYMBOLS
# ════════════════════════════════════════════════════════════
def fetch_nifty500_symbols():
    print("📋 Fetching Nifty 500 symbols...")
    try:
        url = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"
        r = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=20)
        r.raise_for_status()
        df  = pd.read_csv(pd.io.common.BytesIO(r.content))
        col = [c for c in df.columns if "symbol" in c.lower()][0]
        syms = [s.strip() + ".NS" for s in df[col].dropna().tolist()]
        print(f"   ✓ {len(syms)} symbols\n")
        return syms
    except Exception as e:
        print(f"   ⚠️  NSE fetch failed ({e}), using fallback\n")
        return [s+".NS" for s in [
            "RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK","HINDUNILVR","ITC",
            "SBIN","BAJFINANCE","KOTAKBANK","AXISBANK","LT","ASIANPAINT","MARUTI",
            "TITAN","SUNPHARMA","ULTRACEMCO","WIPRO","HCLTECH","ONGC","NTPC",
            "POWERGRID","JSWSTEEL","TATASTEEL","ADANIPORTS","BAJAJ-AUTO","TECHM",
            "EICHERMOT","DRREDDY","DIVISLAB","CIPLA","M&M","TATAMOTORS",
            "NESTLEIND","BRITANNIA","DABUR","MARICO","COALINDIA","HINDALCO","VEDL"
        ]]


def _now_iso():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


def _fetch_index_csv(url):
    """Return {BASE_SYMBOL: {'company':..., 'industry':...}} from an NSE index CSV.
    NSE columns: Company Name, Industry, Symbol, Series, ISIN Code."""
    out = {}
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
        r.raise_for_status()
        df   = pd.read_csv(pd.io.common.BytesIO(r.content))
        cols = {c.lower().strip(): c for c in df.columns}
        symc = next((cols[k] for k in cols if "symbol"  in k), None)
        comc = next((cols[k] for k in cols if "company" in k), None)
        indc = next((cols[k] for k in cols if "industry" in k), None)
        if not symc:
            return out
        for _, row in df.iterrows():
            sym = str(row[symc]).strip()
            if not sym or sym.lower() == "nan":
                continue
            out[sym] = {
                "company":  str(row[comc]).strip() if comc and pd.notna(row[comc]) else sym,
                "industry": str(row[indc]).strip() if indc and pd.notna(row[indc]) else "Other",
            }
    except Exception as e:
        print(f"   ⚠️  index CSV fetch failed ({url.split('/')[-1]}): {e}")
    return out


def build_index_heatmap(n500_symbols):
    """Per-stock heatmap data: % change (from breadth), sector + company (NSE CSV),
    cached market cap (refreshed ~weekly), and Nifty 50 / 500 membership flags."""
    print("🗺️  Building index heatmap...")
    prices = globals().get("_HEATMAP_PRICES") or {}
    if not prices:
        print("   ⚠️  no per-stock moves (breadth didn't run) — skipping\n"); return

    n500_meta = _fetch_index_csv("https://archives.nseindia.com/content/indices/ind_nifty500list.csv")
    n50_set   = set(_fetch_index_csv("https://archives.nseindia.com/content/indices/ind_nifty50list.csv").keys())
    universe_raw = [s.replace(".NS", "") for s in (n500_symbols or [k + ".NS" for k in n500_meta.keys()])]
    # Fetch Nifty 50 caps first so the 50-stock heatmap is fully market-cap sized
    # after a single run; the rest of the 500 fill in over subsequent runs.
    universe = sorted(universe_raw, key=lambda b: (0 if b in n50_set else 1))

    # ── Market-cap cache (refresh weekly, capped per run to avoid a 500-call spike) ──
    from datetime import datetime, timezone, timedelta
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    cap_map, need_refresh = {}, set()
    try:
        rows = sb.table("stock_master").select("symbol,market_cap,market_cap_updated_at").execute().data or []
        for r in rows:
            cap_map[r["symbol"]] = r.get("market_cap")
            ts, fresh = r.get("market_cap_updated_at"), False
            if ts:
                try: fresh = datetime.fromisoformat(str(ts).replace("Z", "+00:00")) > cutoff
                except Exception: pass
            if r.get("market_cap") is None or not fresh:
                need_refresh.add(r["symbol"])
    except Exception as e:
        print(f"   ⚠️  cap cache read: {e}")

    CAP_PER_RUN, refreshed = 150, 0
    for base in universe:
        if refreshed >= CAP_PER_RUN:
            break
        if cap_map.get(base) is None or base in need_refresh:
            mc = fetch_fundamentals(base + ".NS").get("market_cap")
            if mc:
                cap_map[base] = mc
                try:
                    sb.table("stock_master").update(
                        {"market_cap": mc, "market_cap_updated_at": _now_iso()}
                    ).eq("symbol", base).execute()
                except Exception:
                    pass
                refreshed += 1

    rows_out = []
    for base in universe:
        if base not in prices:          # no move data (illiquid / missing) — skip
            continue
        pct, last = prices[base]
        meta = n500_meta.get(base, {})
        ind  = meta.get("industry", "Other") or "Other"
        rows_out.append({
            "symbol":        base,
            "company_name":  meta.get("company", base),
            "sector_name":   ind,
            "sector_key":    ind.lower().replace(" ", "_").replace("&", "and"),
            "pct_change":    pct,
            "last_price":    last,
            "market_cap":    cap_map.get(base),
            "is_nifty50":    base in n50_set,
            "is_nifty500":   True,
            "snapshot_date": TODAY,
            "updated_at":    _now_iso(),
        })
    if not rows_out:
        print("   ⚠️  no heatmap rows built\n"); return
    for i in range(0, len(rows_out), 200):
        try:
            sb.table("index_heatmap").upsert(rows_out[i:i+200], on_conflict="symbol").execute()
        except Exception as e:
            print(f"   ⚠️  heatmap upsert: {e}")
    print(f"   ✓ heatmap: {len(rows_out)} stocks (refreshed {refreshed} caps)\n")


# ════════════════════════════════════════════════════════════
# SECTOR STOCKS — live from NSE CSVs
#
# NSE publishes constituent CSVs at:
# https://archives.nseindia.com/content/indices/ind_[name]list.csv
#
# Each CSV has columns: Company Name, Industry, Symbol, Series, ISIN Code
# We fetch each sector, upsert all stocks into sector_stocks table.
# This runs once per day (full_eod) keeping constituents always current.
# ════════════════════════════════════════════════════════════

# Map: sector_key → (sector_name, category, nse_csv_filename)
SECTOR_CSV_MAP = {
    # Large-cap NSE sectoral indices
    "BANK":        ("Bank Nifty",       "largecap", "ind_niftybanklist.csv"),
    "IT":          ("Nifty IT",         "largecap", "ind_niftyitlist.csv"),
    "AUTO":        ("Nifty Auto",       "largecap", "ind_niftyautolist.csv"),
    "PHARMA":      ("Nifty Pharma",     "largecap", "ind_niftypharmalist.csv"),
    "FMCG":        ("Nifty FMCG",       "largecap", "ind_niftyfmcglist.csv"),
    "METAL":       ("Nifty Metal",      "largecap", "ind_niftymetallist.csv"),
    "REALTY":      ("Nifty Realty",     "largecap", "ind_niftyrealtylist.csv"),
    "ENERGY":      ("Nifty Energy",     "largecap", "ind_niftyenergylist.csv"),
    "INFRA":       ("Nifty Infra",      "largecap", "ind_niftyinfralist.csv"),
    "MEDIA":       ("Nifty Media",      "largecap", "ind_niftymedialist.csv"),
    "PSU_BANK":    ("PSU Bank",         "largecap", "ind_niftypsubanklist.csv"),
    "PVT_BANK":    ("Private Bank",     "largecap", "ind_niftyprivatebanklist.csv"),
    "OIL_GAS":     ("Oil & Gas",        "largecap", "ind_niftyoilgaslist.csv"),
    "HEALTHCARE":  ("Healthcare",       "largecap", "ind_niftyhealthcarelist.csv"),
    "CONS_DUR":    ("Consumer Dur.",    "largecap", "ind_niftyconsumerdurablelist.csv"),
    "FIN_SERV":    ("Fin. Services",    "largecap", "ind_niftyfinancialserviceslist.csv"),
    # Mid/Small cap indices
    "DEFENCE":     ("Defence",          "midsmall", "ind_niftyindiadefencelist.csv"),
    "CHEMICALS":   ("Chemicals",        "midsmall", "ind_niftychemicalslist.csv"),
    "CAP_GOODS":   ("Capital Goods",    "midsmall", "ind_niftycapitalgoodslist.csv"),
    "MFG":         ("Manufacturing",    "midsmall", "ind_niftyindiamfglist.csv"),
}

NSE_BASE = "https://archives.nseindia.com/content/indices/"
NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":     "text/csv,text/plain,*/*",
    "Referer":    "https://www.nseindia.com/",
}

def fetch_sector_stocks_from_nse():
    """
    Fetch constituent stock lists from NSE archives for all sectors.
    Upserts into sector_stocks table. Runs daily to stay current.
    """
    print("📥 Refreshing sector constituents from NSE...")
    session = requests.Session()
    # Warm up cookies
    try:
        session.get("https://www.nseindia.com", headers=NSE_HEADERS, timeout=10)
    except: pass

    total_upserted = 0
    sector_map_result = {}  # {sector_key: [symbols]}

    for sector_key, (sector_name, category, csv_file) in SECTOR_CSV_MAP.items():
        url = NSE_BASE + csv_file
        try:
            r = session.get(url, headers=NSE_HEADERS, timeout=20)
            r.raise_for_status()
            df = pd.read_csv(pd.io.common.BytesIO(r.content))
            df.columns = [c.strip() for c in df.columns]

            # Find symbol and company name columns (flexible matching)
            sym_col  = next((c for c in df.columns if "symbol" in c.lower()), None)
            name_col = next((c for c in df.columns if "company" in c.lower()), None)
            isin_col = next((c for c in df.columns if "isin" in c.lower()), None)

            if not sym_col:
                print(f"   ⚠️  {sector_key}: no Symbol column in {csv_file}"); continue

            rows = []
            symbols = []
            for _, row in df.iterrows():
                sym = str(row[sym_col]).strip().upper()
                if not sym or sym == 'NAN': continue
                symbols.append(sym)
                rows.append({
                    "symbol":       sym,
                    "sector_key":   sector_key,
                    "sector_name":  sector_name,
                    "category":     category,
                    "company_name": str(row[name_col]).strip() if name_col else None,
                    "isin":         str(row[isin_col]).strip() if isin_col else None,
                })

            if rows:
                # Upsert in batches of 100
                for i in range(0, len(rows), 100):
                    sb.table("sector_stocks").upsert(
                        rows[i:i+100], on_conflict="symbol,sector_key"
                    ).execute()
                total_upserted += len(rows)
                sector_map_result[sector_key] = {
                    "name": sector_name, "category": category,
                    "symbols": [s + ".NS" for s in symbols]
                }
                print(f"   ✓ {sector_key}: {len(rows)} stocks from NSE")

        except requests.HTTPError as e:
            print(f"   ⚠️  {sector_key}: HTTP {e.response.status_code} — {csv_file}")
        except Exception as e:
            print(f"   ⚠️  {sector_key}: {e}")

    print(f"   ✓ Total: {total_upserted} stock-sector pairs refreshed\n")
    return sector_map_result


# ════════════════════════════════════════════════════════════
# CMP FOR OPEN TRADES
# ════════════════════════════════════════════════════════════
def fetch_cmp_for_open_trades():
    print("💹 Updating CMP for open trades...")
    cmp_map = {}
    try:
        trades = sb.table("open_trades").select("symbol").execute().data or []
        if not trades:
            print("   No open trades\n"); return cmp_map
        for sym in list({t["symbol"].strip().upper() for t in trades}):
            try:
                price = yf.Ticker(f"{sym}.NS").fast_info.last_price
                cmp   = round(float(price), 2)
                sb.table("open_trades").update(
                    {"cmp": cmp, "updated_at": NOW_IST.isoformat()}
                ).eq("symbol", sym).execute()
                cmp_map[sym] = cmp
                print(f"   ✓ {sym}: ₹{cmp}")
            except Exception as e:
                print(f"   ⚠️  {sym}: {e}")
        print()
    except Exception as e:
        print(f"   ❌ {e}\n")
    return cmp_map


# ════════════════════════════════════════════════════════════
# INDEX LEVELS
# ════════════════════════════════════════════════════════════
INDEX_MAP = {
    "nifty50":      "^NSEI",     "sensex":       "^BSESN",
    "nifty500":     "^CRSLDX",
    "nifty_it":     "^CNXIT",    "nifty_bank":   "^NSEBANK",
    "nifty_pharma": "^CNXPHARMA","nifty_auto":   "^CNXAUTO",
    "nifty_psubank":"^CNXPSUBANK","nifty_metal":  "^CNXMETAL",
    "nifty_realty": "^CNXREALTY","nifty_fmcg":   "^CNXFMCG",
}

# Indices shown in the Key Indices table — only these have *_above_*ema
# columns in index_levels, so only these get EMA flags written.
# (Midcap/Smallcap/Microcap removed — their Yahoo symbols 404 / don't exist.)
EMA_FLAG_INDICES = {
    "nifty50", "sensex", "nifty500",
}

def fetch_index_levels():
    print("📊 Fetching index levels...")
    row = {"snapshot_date": TODAY}
    n500_val = None; nifty50_val = None
    for col_key, ticker in INDEX_MAP.items():
        try:
            # Try longest first (needed for 200-EMA) down to short windows.
            # Some NSE index tickers only serve short periods — accept the
            # first that returns data so the index at least shows level/%chg.
            close = None
            for per in ("1y", "2y", "6mo", "1mo", "5d"):
                c = get_close_series(ticker, period=per)
                if c is not None and len(c) >= 2:
                    close = c
                    if per != "1y":
                        print(f"   ℹ️  {col_key}: used period={per} ({len(c)} bars)")
                    break
            if close is None or len(close) < 2:
                print(f"   ⚠️  {col_key}: not enough data"); continue
            current = round(float(close.iloc[-1]), 2)
            prev    = round(float(close.iloc[-2]), 2)
            row[col_key]           = current
            row[f"{col_key}_prev"] = prev
            # NOTE: Above/Below-EMA flags are NO LONGER written by the pipeline.
            # They are fed manually from the Admin panel into the
            # index_ema_flags table (10 / 21 / 50 / 200-EMA). The pipeline keeps
            # only Day Close (current) and previous close (for % change) here.
            if col_key == "nifty500": n500_val    = current
            if col_key == "nifty50":  nifty50_val = current
            print(f"   ✓ {col_key}: {current:,.2f}")
        except Exception as e:
            print(f"   ⚠️  {col_key}: {e}")
    if len(row) > 1:
        sb.table("index_levels").upsert(row, on_conflict="snapshot_date").execute()
        print(f"   ✓ Saved index_levels\n")
    return row, n500_val, nifty50_val


# ════════════════════════════════════════════════════════════
# PORTFOLIO SNAPSHOT
# ════════════════════════════════════════════════════════════
def save_portfolio_snapshot(cmp_map, n500_val):
    print("📸 Saving model portfolio snapshot...")
    cmp_map = cmp_map or {}          # tolerate a failed cmp fetch (None)
    try:
        admin_uid = get_admin_uid()
        if not admin_uid:
            print("   ⚠️  admin uid not found — skipping snapshot\n"); return
        # MODEL book only (is_model=true). Per-user snapshots use (user_id, snapshot_date);
        # user portfolios are not snapshotted by the pipeline.
        trades = sb.table("open_trades").select("*").eq("is_model", True).execute().data or []
        deployed = unreal = 0.0
        for t in trades:
            qty   = float(t.get("remaining_qty") or t.get("quantity") or 0)
            entry = float(t.get("avg_entry_price") or t.get("entry_price") or 0)
            cmp   = float(cmp_map.get(t["symbol"].upper(), t.get("cmp") or entry))
            deployed += entry * qty
            unreal   += (cmp - entry) * qty
        # Base capital — single source of truth: the admin's portfolio_config
        # row (editable in the Admin panel). Falls back to the TOTAL_CAPITAL env,
        # then to 85L. Change this ONLY for real capital in/out — trading profit
        # already compounds into portfolio_value below, so it must not be folded
        # into the base (that would double-count it).
        total_cap = None
        try:
            cfg = sb.table("portfolio_config").select("total_capital").eq("user_id", admin_uid).limit(1).execute().data or []
            if cfg and cfg[0].get("total_capital"):
                total_cap = float(cfg[0]["total_capital"])
        except Exception:
            total_cap = None
        if not total_cap:
            total_cap = float(os.environ.get("TOTAL_CAPITAL", 8500000))
        cash_avail = max(0.0, total_cap - deployed)
        # Realized P&L from booked (closed) model trades — so the portfolio
        # value and return reflect BOTH booked and open gains, not just open.
        realised = 0.0
        try:
            closed = sb.table("closed_trades").select("realised_pnl").eq("is_model", True).execute().data or []
            realised = sum(float(c.get("realised_pnl") or 0) for c in closed)
        except Exception:
            realised = 0.0
        port_val   = total_cap + realised + unreal
        cum_ret    = round((port_val / total_cap - 1) * 100, 4) if total_cap else 0.0
        sb.table("portfolio_snapshots").upsert({
            "user_id":               admin_uid,
            "is_model":              True,
            "snapshot_date":         TODAY,
            "portfolio_value":       round(port_val, 2),
            "total_capital":         round(total_cap, 2),
            "cash_available":        round(cash_avail, 2),
            "nifty500_level":        n500_val,
            "cumulative_return_pct": cum_ret,
        }, on_conflict="user_id,is_model,snapshot_date").execute()
        print(f"   ✓ Model portfolio ₹{port_val:,.0f} | cash ₹{cash_avail:,.0f}\n")
    except Exception as e:
        print(f"   ⚠️  Snapshot failed: {e}\n")


# ════════════════════════════════════════════════════════════
# PER-USER PERFORMANCE STATS  (privacy-preserving aggregates)
# ------------------------------------------------------------
# Runs with the service key, so it can read every user's PRIVATE book to
# compute ONE aggregated row per user in `user_stats`. The admin panel reads
# only this table — never the raw trades. No micromanagement, just the signals
# needed to coach users toward the MSTP course.
# ════════════════════════════════════════════════════════════
def compute_user_stats(cmp_map):
    print("📊 Computing per-user performance stats...")
    cmp_map = cmp_map or {}
    try:
        from datetime import datetime as _dt, timedelta as _td
        # 1. Distinct users who have a personal book (is_model = false).
        open_rows   = sb.table("open_trades").select("*").eq("is_model", False).execute().data or []
        closed_rows = sb.table("closed_trades").select("*").eq("is_model", False).execute().data or []
        cfg_rows    = sb.table("portfolio_config").select("user_id,total_capital").execute().data or []
        cap_by_user = {r["user_id"]: float(r.get("total_capital") or 0) for r in cfg_rows if r.get("user_id")}

        uids = set()
        for r in open_rows + closed_rows:
            if r.get("user_id"): uids.add(r["user_id"])
        uids |= set(cap_by_user.keys())
        if not uids:
            print("   • no user books yet — nothing to compute\n"); return

        today = _dt.utcnow().date()
        cutoff_30 = today - _td(days=30)

        def _d(s):
            try: return _dt.fromisoformat(str(s)[:10]).date()
            except Exception: return None

        written = 0
        for uid in uids:
            u_open   = [t for t in open_rows   if t.get("user_id") == uid]
            u_closed = [t for t in closed_rows if t.get("user_id") == uid]

            # profile (email / name / tier) — defensive select("*")
            email = full_name = tier = None
            try:
                pr = sb.table("profiles").select("*").eq("user_id", uid).limit(1).execute().data
                if pr:
                    p = pr[0]
                    email     = p.get("email")
                    full_name = p.get("full_name") or p.get("name")
                    tier      = p.get("tier")
            except Exception:
                pass

            # ── activity ──
            open_positions = len(u_open)
            total_closed   = len(u_closed)
            exits = [(_d(t.get("exit_date")), t) for t in u_closed]
            trades_30d = sum(1 for d, _ in exits if d and d >= cutoff_30)
            entry_dates = [_d(t.get("entry_date")) for t in (u_open + u_closed) if _d(t.get("entry_date"))]
            exit_dates  = [d for d, _ in exits if d]
            first_trade = min(entry_dates) if entry_dates else None
            last_trade  = max(exit_dates) if exit_dates else (max(entry_dates) if entry_dates else None)

            # ── performance (closed trades) ──
            wins   = [t for t in u_closed if float(t.get("realised_pnl") or 0) > 0]
            losses = [t for t in u_closed if float(t.get("realised_pnl") or 0) <= 0]
            gross_win  = sum(float(t.get("realised_pnl") or 0) for t in wins)
            gross_loss = abs(sum(float(t.get("realised_pnl") or 0) for t in losses))
            win_rate      = round(len(wins) / total_closed * 100, 1) if total_closed else 0.0
            profit_factor = round(gross_win / gross_loss, 2) if gross_loss > 0 else (round(gross_win, 2) if gross_win else 0.0)
            avg_win_pct   = round(sum(float(t.get("return_pct") or 0) for t in wins)   / len(wins),   2) if wins   else 0.0
            avg_loss_pct  = round(sum(float(t.get("return_pct") or 0) for t in losses) / len(losses), 2) if losses else 0.0
            expectancy    = round((win_rate/100)*avg_win_pct + ((100-win_rate)/100)*avg_loss_pct, 2)
            realised_list = [float(t.get("realised_pnl") or 0) for t in u_closed]
            max_profit    = round(max(realised_list), 2) if realised_list else 0.0
            max_loss      = round(min(realised_list), 2) if realised_list else 0.0
            total_realised= round(sum(realised_list), 2)

            # ── discipline signals ──
            with_sl = sum(1 for t in u_closed if t.get("sl_price") not in (None, 0, "0"))
            pct_with_sl = round(with_sl / total_closed * 100, 1) if total_closed else 0.0
            holds = [float(t.get("holding_days")) for t in u_closed if t.get("holding_days") not in (None, "")]
            avg_hold = round(sum(holds)/len(holds), 1) if holds else 0.0

            # max drawdown on the cumulative-realised equity curve (exit order)
            total_capital = cap_by_user.get(uid, 0.0)
            ordered = sorted([(d, t) for d, t in exits if d], key=lambda x: x[0])
            eq = total_capital if total_capital else max(1.0, sum(abs(x) for x in realised_list))
            peak = eq; max_dd = 0.0
            for _, t in ordered:
                eq += float(t.get("realised_pnl") or 0)
                if eq > peak: peak = eq
                if peak > 0:
                    dd = (eq - peak) / peak * 100
                    if dd < max_dd: max_dd = dd
            max_drawdown_pct = round(max_dd, 2)

            # ── exposure (open book) ──
            deployed = 0.0
            for t in u_open:
                ae = float(t.get("avg_entry_price") or t.get("entry_price") or 0)
                rq = float(t.get("remaining_qty") or t.get("quantity") or 0)
                deployed += ae * rq
            deployed = round(deployed, 2)
            exposure_pct = round(deployed / total_capital * 100, 1) if total_capital > 0 else 0.0

            sb.table("user_stats").upsert({
                "user_id": uid, "email": email, "full_name": full_name, "tier": tier,
                "open_positions": open_positions, "total_closed_trades": total_closed,
                "trades_last_30d": trades_30d,
                "first_trade_date": first_trade.isoformat() if first_trade else None,
                "last_trade_date":  last_trade.isoformat()  if last_trade  else None,
                "win_rate": win_rate, "profit_factor": profit_factor, "expectancy_pct": expectancy,
                "avg_win_pct": avg_win_pct, "avg_loss_pct": avg_loss_pct,
                "max_profit": max_profit, "max_loss": max_loss, "total_realised_pnl": total_realised,
                "pct_trades_with_sl": pct_with_sl, "avg_holding_days": avg_hold,
                "max_drawdown_pct": max_drawdown_pct, "total_capital": total_capital,
                "deployed": deployed, "exposure_pct": exposure_pct,
                "updated_at": _dt.utcnow().isoformat(),
            }, on_conflict="user_id").execute()
            written += 1

        print(f"   ✓ user_stats updated for {written} user(s)\n")
    except Exception as e:
        print(f"   ⚠️  user_stats failed: {e}\n")


# ════════════════════════════════════════════════════════════
# PROPRIETARY MARKET SCORE  (9-indicator, two-layer engine)
# ════════════════════════════════════════════════════════════
# Private swing-trading exposure model. NOT investment advice — it sizes the
# AGGREGATE book; per-trade stops / position risk still govern each position.
#
#   REGIME layer  (slow) → sets the exposure CEILING (what you're ALLOWED to hold)
#   TACTICAL layer (fast) → dials position WITHIN that ceiling (timing)
#
# All 9 inputs normalise to 0..100 (50 = neutral). The two layers combine into a
# composite, which is 3-day-EMA smoothed, mapped to a suggested exposure with hard
# vetoes, then run through asymmetric hysteresis (de-risk fast, add slow).

# Key Indices feeding the index-trend pillar (must match admin IDX_ADMIN list).
KEY_INDEX_KEYS = [
    "nifty50", "sensex", "nifty500",
    "nifty_midcap100", "nifty_smallcap100", "nifty_microcap250", "nifty_alpha50",
]


def _norm_ratio(a, b, min_sample=0):
    """Self-scaling 0..100 where 50 = balanced: a/(a+b)*100.
    Returns neutral 50 when the sample is empty or below min_sample."""
    a = a or 0; b = b or 0
    tot = a + b
    if tot <= 0 or tot < min_sample:
        return 50.0
    return a / tot * 100.0


def _index_trend_score():
    """Key Indices pillar. For each index, longer EMAs weigh heavier:
        t = (1·a10 + 2·a21 + 3·a50 + 4·a200) / 10   (1.0 = above all, 0.0 = below all)
    Averaged across indices that HAVE flags set, ×100. Indices the admin hasn't
    flagged are skipped (not scored as 0, which would falsely drag it bearish).
    Returns (score_0_100, n_indices_used)."""
    try:
        rows = sb.table("index_ema_flags").select("*").execute().data or []
    except Exception:
        rows = []
    ts = []
    for r in rows:
        if r.get("index_key") not in KEY_INDEX_KEYS:
            continue
        flags = [r.get("above_10ema"), r.get("above_21ema"),
                 r.get("above_50ema"), r.get("above_200ema")]
        if all(f is None for f in flags):
            continue  # not set yet — don't dilute
        a10, a21, a50, a200 = [1 if f else 0 for f in flags]
        ts.append((1*a10 + 2*a21 + 3*a50 + 4*a200) / 10.0)
    if not ts:
        return 50.0, 0
    return sum(ts) / len(ts) * 100.0, len(ts)


def _fii_dii_flow5d(scale=25000.0):
    """FII/DII pillar. 5-day rolling net, FII weighted heavier than DII (foreign
    flows drive Indian swing moves more; DII is steadier). Squashed with tanh so
    extreme single days stay bounded:
        flow5d = 0.7·ΣFII(5d) + 0.3·ΣDII(5d)
        score  = 50 + 50·tanh(flow5d / scale)
    Returns (score_0_100, flow5d_cr)."""
    try:
        rows = (sb.table("fii_dii_activity")
                  .select("fii_cash_net,dii_cash_net,activity_date")
                  .order("activity_date", desc=True).limit(5).execute().data or [])
    except Exception:
        rows = []
    if not rows:
        return 50.0, 0.0
    fii = sum(float(r.get("fii_cash_net") or 0) for r in rows)
    dii = sum(float(r.get("dii_cash_net") or 0) for r in rows)
    flow5d = 0.7 * fii + 0.3 * dii
    score  = 50.0 + 50.0 * float(np.tanh(flow5d / scale))
    return score, flow5d


def _prev_breadth_row():
    """Most recent market_breadth row BEFORE today — supplies prior smoothed
    composite (for the 3-day EMA), prior exposure (for hysteresis), and prior
    200EMA breadth (for the falling-breadth veto). Excluding today means intraday
    re-runs don't compound the smoothing within a single session."""
    try:
        rows = (sb.table("market_breadth").select(
                    "pct_above_200ema,composite_smoothed,suggested_exposure_pct")
                  .lt("snapshot_date", TODAY)
                  .order("snapshot_date", desc=True).limit(1).execute().data or [])
        return rows[0] if rows else None
    except Exception:
        return None


def compute_market_score(p21, p50, p200, adv, dec, h52, l52, pdh, pdl,
                         strong_up, strong_down, india_vix):
    """Compute the full 9-pillar score. Returns a dict of fields ready to merge
    into the market_breadth upsert payload."""
    # ── 1. Normalise the 9 pillars to 0..100 (50 = neutral) ──
    n_21  = float(p21  if p21  is not None else 50)   # already a percentage
    n_50  = float(p50  if p50  is not None else 50)
    n_200 = float(p200 if p200 is not None else 50)
    n_ad  = _norm_ratio(adv, dec)                              # direction only
    n_52  = _norm_ratio(h52, l52, min_sample=20)              # guard thin samples
    n_pdh = _norm_ratio(pdh, pdl)                             # prev-day H/L
    n_thr = _norm_ratio(strong_up, strong_down, min_sample=10) # breadth thrust
    n_idx, n_idx_used = _index_trend_score()                  # key-index trend
    n_fii, flow5d     = _fii_dii_flow5d()                     # 5-day FII/DII flow

    # ── 2. Two layers ──
    # Regime (slow) — the environment; sets how much you're ALLOWED to deploy.
    R = 0.40 * n_200 + 0.35 * n_idx + 0.25 * n_50
    # Tactical (fast) — timing within that ceiling.
    T = (0.20 * n_21 + 0.20 * n_ad + 0.20 * n_52
         + 0.15 * n_pdh + 0.10 * n_thr + 0.15 * n_fii)
    # Headline composite — regime weighted more (horizon is swings, not scalps).
    composite = 0.55 * R + 0.45 * T

    # ── 3. Smooth the composite (3-day EMA, alpha = 2/(3+1) = 0.5) ──
    prev          = _prev_breadth_row() or {}
    prev_smoothed = prev.get("composite_smoothed")
    composite_smoothed = (composite if prev_smoothed is None
                          else 0.5 * composite + 0.5 * float(prev_smoothed))

    # ── 4. Regime → exposure ceiling; tactical dials within it ──
    if   R >= 70: ceiling = 100
    elif R >= 55: ceiling = 80
    elif R >= 45: ceiling = 50
    elif R >= 30: ceiling = 25
    else:         ceiling = 10
    expo = ceiling * (0.4 + 0.6 * T / 100.0)   # 0.4 floor keeps you partly in on soft days

    # ── 5. Hard vetoes — override the score on dangerous tapes ──
    vetoes = []
    prev_p200 = prev.get("pct_above_200ema")
    if n_200 < 40 and prev_p200 is not None and n_200 < float(prev_p200):
        expo = min(expo, 20); vetoes.append("Weak & falling 200EMA breadth")
    if india_vix is not None:
        if india_vix >= 20:
            expo = min(expo, 25); vetoes.append(f"High VIX ({india_vix})")
        elif india_vix >= 16:
            expo = min(expo, 60); vetoes.append(f"Elevated VIX ({india_vix})")
    if flow5d < -20000:
        expo = min(expo, 50); vetoes.append("Sustained FII/DII outflow (5d)")

    # ── 6. Asymmetric hysteresis — de-risk fast, add slow ──
    prev_expo = prev.get("suggested_exposure_pct")
    if prev_expo is None:
        exposure = expo
    elif expo < float(prev_expo):
        exposure = expo                                          # cut now
    else:
        exposure = float(prev_expo) + (expo - float(prev_expo)) * 0.5  # add half-step
    exposure = max(0, min(100, round(exposure / 5.0) * 5))       # clean 5% bands

    # ── 7. Headline signal (off the SMOOTHED composite) ──
    cs = composite_smoothed
    signal = ("Strong Bull" if cs >= 70 else "Bullish" if cs >= 55
              else "Neutral" if cs >= 45 else "Cautious" if cs >= 30 else "Defensive")

    breakdown = {
        "pillars": {
            "above21ema": round(n_21, 1), "above50ema": round(n_50, 1),
            "above200ema": round(n_200, 1), "advance_decline": round(n_ad, 1),
            "high_low_52w": round(n_52, 1), "prev_day_hl": round(n_pdh, 1),
            "thrust": round(n_thr, 1), "key_indices": round(n_idx, 1),
            "fii_dii": round(n_fii, 1),
        },
        "regime": round(R, 1), "tactical": round(T, 1), "ceiling": ceiling,
        "exposure_raw": round(expo, 1), "flow5d_cr": round(flow5d, 0),
        "indices_used": n_idx_used, "thrust_counts": [strong_up, strong_down],
        "vetoes": vetoes,
    }

    return {
        "regime_score":           round(R, 2),
        "tactical_score":         round(T, 2),
        "composite_score":        round(composite, 2),
        "composite_smoothed":     round(composite_smoothed, 2),
        "suggested_exposure_pct": exposure,
        "market_signal":          signal,
        "score_breakdown":        breakdown,
    }


# ── Plain-language rationale for the dashboard "Rationale" dropdown ──
# Two layers: a deterministic summary built from the computed numbers (ALWAYS
# correct, always available), optionally rephrased by Claude into friendlier
# prose grounded in those same numbers. The card's numbers come from the data,
# never from this text — so display is accurate regardless of the AI path.

_PILLAR_LABELS = {
    "above21ema": "short-term trend (21 EMA)",
    "above50ema": "medium-term trend (50 EMA)",
    "above200ema": "long-term trend (200 EMA)",
    "advance_decline": "advance/decline",
    "high_low_52w": "52-week highs vs lows",
    "prev_day_hl": "previous-day range breaks",
    "thrust": "breadth thrust",
    "key_indices": "key-index trend",
    "fii_dii": "institutional flows (FII/DII)",
}


def _join(items):
    items = list(items)
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    return ", ".join(items[:-1]) + " and " + items[-1]


def _deterministic_rationale(score):
    b = score["score_breakdown"]; p = b["pillars"]
    sig  = score["market_signal"]
    comp = score["composite_smoothed"]
    R, T = b["regime"], b["tactical"]
    ranked = sorted(p.items(), key=lambda kv: kv[1], reverse=True)
    strong = [_PILLAR_LABELS[k] for k, v in ranked if v >= 55][:3]
    weak   = [_PILLAR_LABELS[k] for k, v in reversed(ranked) if v < 45][:3]
    env = ("a supportive backdrop" if R >= 55 else
           "a fragile backdrop" if R < 45 else "a mixed backdrop")
    tac = ("buyers in control" if T >= 55 else
           "sellers pressing" if T < 45 else "a balanced tape")
    parts = [f"The market reads {sig} at {comp:.0f} out of 100.",
             f"The slow regime layer ({R:.0f}) points to {env}, "
             f"while the fast tactical layer ({T:.0f}) shows {tac}."]
    if strong:
        parts.append("Support is coming from " + _join(strong) + ".")
    if weak:
        parts.append("The main drag is " + _join(weak) + ".")
    if b["vetoes"]:
        parts.append("Risk guard active: " + "; ".join(b["vetoes"]) + ".")
    return " ".join(parts)


def _ai_rationale(score, facts):
    """Rephrase the deterministic facts into friendly retail prose via Claude.
    Returns None on any failure (caller falls back to the deterministic text)."""
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        return None
    model = os.environ.get("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")
    b = score["score_breakdown"]
    prompt = (
        "You are writing a short explainer for a stock-market breadth dashboard "
        "read by retail swing traders. Using ONLY the facts below, write 2-3 "
        "plain-English sentences explaining what today's Market Score means and "
        "why. Keep it simple and calm. Do NOT invent any numbers or facts beyond "
        "those given. Do NOT give buy/sell advice or price targets. No emojis.\n\n"
        f"Signal: {score['market_signal']}\n"
        f"Composite score: {score['composite_smoothed']:.0f}/100 "
        f"(regime {b['regime']:.0f}, tactical {b['tactical']:.0f})\n"
        f"Deterministic summary to rephrase: {facts}"
    )
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": key,
                     "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={"model": model, "max_tokens": 240,
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=20,
        )
        r.raise_for_status()
        j = r.json()
        txt = "".join(blk.get("text", "") for blk in j.get("content", [])
                      if blk.get("type") == "text").strip()
        return txt or None
    except Exception as e:
        print(f"   ℹ️  AI rationale unavailable ({e}) — using deterministic summary")
        return None


def build_rationale(score):
    """Deterministic summary always; AI polish only on the meaningful EOD/flow
    runs (keeps cost negligible and intraday runs fast)."""
    facts = _deterministic_rationale(score)
    if RUN_MODE in ("full_eod", "fii_dii"):
        ai = _ai_rationale(score, facts)
        if ai:
            return ai
    return facts


# ════════════════════════════════════════════════════════════
# DAILY MARKET BRIEF  (pre-market, ~5:30 IST)
# ------------------------------------------------------------
# India-first macro brief via Claude + live web search. One call/day.
# Upserts public.market_brief. Skips if the admin hand-edited today's brief.
# Requires ANTHROPIC_API_KEY (web search must be enabled in the Anthropic
# Console). Model is configurable via ANTHROPIC_BRIEF_MODEL.
# ════════════════════════════════════════════════════════════
def generate_market_brief(only_if_missing=False):
    import json as _json
    print("📰 Generating Daily Market Brief...")
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        print("   ℹ️  ANTHROPIC_API_KEY not set — skipping brief.\n"); return
    brief_date = NOW_IST.date().isoformat()

    # Respect a manual admin edit for today — never overwrite it.
    try:
        ex = sb.table("market_brief").select("source").eq("brief_date", brief_date).limit(1).execute().data
        if ex and ex[0].get("source") == "manual":
            print("   ℹ️  Today's brief was edited by admin — leaving it.\n"); return
        # EOD safety-net: if the morning pre-market run already published today's
        # brief, don't regenerate it post-market.
        if only_if_missing and ex:
            print("   ℹ️  Today's brief already exists — EOD backfill not needed.\n"); return
    except Exception:
        pass

    # NOTE: `or` (not get-default) so an empty env value falls back too.
    model = os.environ.get("ANTHROPIC_BRIEF_MODEL") or "claude-sonnet-4-6"
    _today_str = NOW_IST.strftime("%A, %d %B %Y")
    prompt = (
        f"Today's date is {_today_str} (India Standard Time). Treat this as the current day; "
        "do not guess or infer a different date, and refer to weekdays correctly relative to it.\n\n"
        "You are the market desk for an Indian swing-trading platform. Use web search to find the "
        "most important and LATEST market-moving news as of this morning (India time), then write a "
        "concise PRE-MARKET brief for retail swing traders.\n\n"
        "Coverage priority:\n"
        "1) INDIA FIRST: Nifty/Sensex direction and key levels, RBI / monetary policy, Indian macro "
        "(inflation, GDP, IIP, rupee), government policy / budget / regulation, notable Indian sectors "
        "and large-cap movers, FII/DII flows.\n"
        "2) Lightly cover global cues that affect India: US markets / Fed, crude oil, USDINR, major "
        "geopolitics (only as it impacts Indian markets).\n\n"
        "Be BRIEF and factual. No buy/sell advice, no price targets, no emojis. Do not invent anything; "
        "base everything on what you find in search.\n\n"
        "Return ONLY a JSON object (no markdown fences, no text around it) with EXACTLY this shape:\n"
        "{\n"
        '  "overall_sentiment": "bullish" | "neutral" | "bearish",\n'
        '  "summary": "2-3 short paragraphs, separated by a blank line",\n'
        '  "themes": [ {"text": "one concise line", "sentiment": "bullish|neutral|bearish"} ],  // 6-9 items\n'
        '  "sources": [ {"title": "short name", "url": "https://..."} ]  // up to 5\n'
        "}"
    )
    r = None
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={
                "model": model, "max_tokens": 2200,
                "tools": [{"type": "web_search_20250305", "name": "web_search", "max_uses": 6}],
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=150,
        )
        if r.status_code >= 400:
            # surface the API's actual error message instead of a bare status code
            print(f"   ⚠️  Market brief API {r.status_code}: {r.text[:600]}\n")
            return
        j = r.json()
        txt = "".join(blk.get("text", "") for blk in j.get("content", [])
                      if blk.get("type") == "text").strip()
        # tolerate code fences / surrounding prose — extract the JSON object
        if "```" in txt:
            seg = txt.split("```")
            txt = seg[1] if len(seg) > 1 else txt
            if txt.lstrip()[:4].lower() == "json":
                txt = txt.lstrip()[4:]
        a, z = txt.find("{"), txt.rfind("}")
        if a != -1 and z != -1:
            txt = txt[a:z + 1]
        data = _json.loads(txt)

        sentiment = str(data.get("overall_sentiment", "neutral")).lower()
        if sentiment not in ("bullish", "neutral", "bearish"):
            sentiment = "neutral"
        themes = []
        for t in (data.get("themes") or []):
            if isinstance(t, dict) and t.get("text"):
                s = str(t.get("sentiment", "neutral")).lower()
                themes.append({"text": str(t["text"]),
                               "sentiment": s if s in ("bullish", "neutral", "bearish") else "neutral"})
        sources = [{"title": str(s.get("title", "")), "url": str(s.get("url"))}
                   for s in (data.get("sources") or [])
                   if isinstance(s, dict) and s.get("url")][:5]

        sb.table("market_brief").upsert({
            "brief_date": brief_date,
            "overall_sentiment": sentiment,
            "summary": str(data.get("summary", "")),
            "themes": themes,
            "sources": sources,
            "source": "auto",
            "updated_at": datetime.now(IST).isoformat(),
        }, on_conflict="brief_date").execute()
        print(f"   ✓ Market brief published ({sentiment}, {len(themes)} themes)\n")
    except Exception as e:
        print(f"   ⚠️  Market brief failed: {e}\n")


# ════════════════════════════════════════════════════════════
# MARKET BREADTH
# ════════════════════════════════════════════════════════════
def calculate_market_breadth(symbols, nifty50_close=None):
    if not symbols:
        print("   ⚠️  No symbols (upstream fetch failed) — skipping breadth.")
        return
    print(f"🔬 Computing market breadth ({len(symbols)} stocks)...")
    a21=a50=a200=h52=l52=adv=dec=unch=valid = 0
    pdh=pdl = 0
    strong_up=strong_down = 0          # breadth-thrust tallies (|move| ≥ 3%)
    movers = []  # (symbol, pct_change, last_price) — for top gainers / losers
    BATCH = 50
    for i in range(0, len(symbols), BATCH):
        batch = symbols[i:i+BATCH]
        try:
            raw = yf.download(batch, period="2y", progress=False,
                              group_by="ticker", auto_adjust=True)
            for sym in batch:
                try:
                    if len(batch) == 1:
                        cl = raw["Close"]
                        if isinstance(cl, pd.DataFrame): cl = cl.iloc[:, 0]
                    else:
                        if sym not in raw.columns.get_level_values(0): continue
                        cl = raw[sym]["Close"]
                        if isinstance(cl, pd.DataFrame): cl = cl.iloc[:, 0]
                    cl = cl.dropna()
                    if len(cl) < 50: continue
                    valid += 1
                    cur, prev = float(cl.iloc[-1]), float(cl.iloc[-2])
                    if cur > prev:   adv  += 1
                    elif cur < prev: dec  += 1
                    else:            unch += 1
                    # Previous Day High / Low breadth: is the latest price above the
                    # prior session's HIGH (bullish breakout) or below its LOW?
                    try:
                        if len(batch) == 1:
                            hi = raw["High"]; lo = raw["Low"]
                        else:
                            hi = raw[sym]["High"]; lo = raw[sym]["Low"]
                        if isinstance(hi, pd.DataFrame): hi = hi.iloc[:, 0]
                        if isinstance(lo, pd.DataFrame): lo = lo.iloc[:, 0]
                        hi = hi.reindex(cl.index); lo = lo.reindex(cl.index)
                        prev_high = float(hi.iloc[-2]); prev_low = float(lo.iloc[-2])
                        if pd.notna(prev_high) and cur > prev_high: pdh += 1
                        if pd.notna(prev_low)  and cur < prev_low:  pdl += 1
                    except Exception:
                        pass
                    if prev > 0:
                        pct = (cur - prev) / prev * 100
                        movers.append((sym.replace('.NS', ''), round(pct, 2), round(cur, 2)))
                        # Breadth thrust: count conviction moves across all Nifty 500.
                        # Flat ±3% absolute threshold (swing-trader's "strong" lens).
                        if   pct >=  3.0: strong_up   += 1
                        elif pct <= -3.0: strong_down += 1
                    if cur > float(compute_ema(cl,21).iloc[-1]):  a21  += 1
                    if cur > float(compute_ema(cl,50).iloc[-1]):  a50  += 1
                    if cur > float(compute_ema(cl,200).iloc[-1]): a200 += 1
                    # 52-week high/low over up to 252 trading days.
                    # Use a window no larger than the data we actually have,
                    # so stocks with <252 bars still get a valid (shorter) range
                    # instead of NaN (which silently zeroed all breakouts before).
                    win  = min(252, len(cl))
                    hi52 = float(cl.rolling(win).max().iloc[-1])
                    lo52 = float(cl.rolling(win).min().iloc[-1])
                    if cur >= hi52*0.97: h52 += 1
                    if cur <= lo52*1.03: l52 += 1
                except: continue
        except Exception as e:
            print(f"   ⚠️  Batch {i//BATCH+1}: {e}")
    if not valid:
        print("   ⚠️  No valid data\n"); return
    p21  = round(a21/valid*100, 2)
    p50  = round(a50/valid*100, 2)
    p200 = round(a200/valid*100, 2)
    india_vix = None
    try:
        vix_cl = get_close_series("^INDIAVIX", period="5d")
        if len(vix_cl): india_vix = round(float(vix_cl.iloc[-1]), 2)
    except: pass
    # Expose the full per-stock move map so the heatmap step can reuse it
    # (avoids re-downloading prices for all 500 names).
    globals()["_HEATMAP_PRICES"] = { s: (p, c) for (s, p, c) in movers }
    movers_sorted = sorted(movers, key=lambda m: m[1], reverse=True)
    top_gainers = [{"s": s, "p": p, "c": c} for s, p, c in movers_sorted if p > 0][:50]
    top_losers  = [{"s": s, "p": p, "c": c} for s, p, c in reversed(movers_sorted) if p < 0][:50]

    # ── Proprietary Market Score (9-pillar, two-layer) ──
    # Reads index_ema_flags + fii_dii_activity + prior breadth row internally.
    score = compute_market_score(
        p21, p50, p200, adv, dec, h52, l52, pdh, pdl,
        strong_up, strong_down, india_vix,
    )
    score["score_rationale"] = build_rationale(score)

    payload = {
        "snapshot_date":    TODAY,
        "total_stocks":     valid,
        "advancing":        adv, "declining": dec, "unchanged": unch,
        "pct_above_21ema":  p21,  "above_21ema_count":  a21,
        "pct_above_50ema":  p50,  "above_50ema_count":  a50,
        "pct_above_200ema": p200, "above_200ema_count": a200,
        "new_52w_highs":    h52,  "new_52w_lows":       l52,
        "above_pdh":        pdh,  "below_pdl":          pdl,
        "strong_up_count":  strong_up, "strong_down_count": strong_down,
        "top_gainers":      top_gainers,
        "top_losers":       top_losers,
        "nifty50_close":    nifty50_close,
        "india_vix":        india_vix,
    }
    payload.update(score)
    sb.table("market_breadth").upsert(payload, on_conflict="snapshot_date").execute()
    # Free-readable teaser (headline score + signal + brief) for the upgrade/MSTP funnel
    try:
        sb.table("market_teaser").upsert({
            "snapshot_date":      TODAY,
            "composite_smoothed": payload.get("composite_smoothed"),
            "market_signal":      payload.get("market_signal"),
            "rationale":          payload.get("score_rationale"),
        }, on_conflict="snapshot_date").execute()
    except Exception as e:
        print(f"   ⚠️  market_teaser: {e}")
    print(f"   ✓ {valid} stocks | 21:{p21}% 50:{p50}% 200:{p200}% | PDH:{pdh} PDL:{pdl} | VIX:{india_vix}")
    print(f"   📊 Score {score['composite_score']} (smoothed {score['composite_smoothed']}) "
          f"· {score['market_signal']} · R{score['regime_score']}/T{score['tactical_score']} "
          f"· thrust {strong_up}↑/{strong_down}↓ · exposure {score['suggested_exposure_pct']}%"
          + (f" · vetoes: {', '.join(score['score_breakdown']['vetoes'])}"
             if score['score_breakdown']['vetoes'] else "") + "\n")


# ════════════════════════════════════════════════════════════
# FII / DII
# ════════════════════════════════════════════════════════════
def fetch_fii_dii(nifty_close=None):
    print("🏦 Fetching FII/DII data...")
    data = None
    last_err = None
    # NSE's unofficial endpoint is flaky from automated IPs. Try a few times,
    # warming cookies each attempt. If all fail, we SKIP the write rather than
    # overwriting good history with zeros.
    for attempt in range(3):
        try:
            sess = requests.Session()
            hdrs = {
                "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                               "AppleWebKit/537.36 (KHTML, like Gecko) "
                               "Chrome/120.0 Safari/537.36"),
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
            }
            # Warm cookies on the homepage + the FII/DII report page
            sess.get("https://www.nseindia.com", headers=hdrs, timeout=12)
            sess.get("https://www.nseindia.com/reports-indices-historical-fii-dii",
                     headers=hdrs, timeout=12)
            r = sess.get("https://www.nseindia.com/api/fiidiiTradeReact",
                         headers={**hdrs, "Referer": "https://www.nseindia.com/"},
                         timeout=15)
            r.raise_for_status()
            j = r.json()
            if isinstance(j, list) and len(j) > 0:
                data = j
                break
            last_err = "empty response"
        except Exception as e:
            last_err = str(e)
        time.sleep(3)  # backoff between attempts

    if not data:
        # CRITICAL: do NOT upsert zeros — that would overwrite good history.
        print(f"   ⚠️  FII/DII unavailable ({last_err}) — skipping write (keeping existing data)\n")
        return

    def _num(v):
        try:
            t = str(v).replace(",", "").strip()
            return float(t) if t not in ("", "-", "None") else None
        except (TypeError, ValueError):
            return None

    fii_net = dii_net = None
    for row in data:
        cat  = str(row.get("category", "")).upper()
        # TRUE net = gross buy − gross sell (sign-correct: sell-heavy → negative).
        # NSE's fiidiiTradeReact exposes buyValue/sellValue; older feeds use
        # netValue. We deliberately do NOT trust "netPurchases" alone — it is a
        # purchases-side (positive) figure and was the cause of FII selling
        # showing up as a large positive inflow.
        buy  = _num(row.get("buyValue",  row.get("buyAmount")))
        sell = _num(row.get("sellValue", row.get("sellAmount")))
        if buy is not None and sell is not None:
            val = buy - sell
        else:
            val = _num(row.get("netValue",
                       row.get("netPurchaseSales",
                       row.get("netPurchases", row.get("netSales")))))
        if val is None:
            continue
        if "FII" in cat or "FPI" in cat:
            fii_net = val
        elif "DII" in cat:
            dii_net = val

    if fii_net is None and dii_net is None:
        print("   ⚠️  FII/DII parsed but no FII/DII rows found — skipping write\n")
        return

    try:
        row = {
            "activity_date": TODAY,
            "fii_cash_net":  fii_net if fii_net is not None else 0.0,
            "dii_cash_net":  dii_net if dii_net is not None else 0.0,
            "source":        "NSE",
        }
        if nifty_close is not None:
            row["nifty_close"] = nifty_close
        sb.table("fii_dii_activity").upsert(row, on_conflict="activity_date").execute()
        print(f"   ✓ FII: ₹{(fii_net or 0):,.2f}Cr | DII: ₹{(dii_net or 0):,.2f}Cr"
              + (f" | Nifty {nifty_close:,.0f}" if nifty_close else "") + "\n")
    except Exception as e:
        print(f"   ⚠️  FII/DII DB write failed: {e}\n")


# ════════════════════════════════════════════════════════════
# CANDLES
# ════════════════════════════════════════════════════════════
def fetch_candles_for_open_trades():
    print("🕯️  Fetching candles for open trades...")
    try:
        trades = sb.table("open_trades").select("symbol").execute().data or []
        if not trades: print("   No open trades\n"); return
        for sym in list({t["symbol"].strip().upper() for t in trades}):
            try:
                hist = yf.Ticker(f"{sym}.NS").history(period="180d", interval="1d", auto_adjust=True)
                if hist.empty: continue
                rows = [{"symbol":sym,"date":str(dt.date()),
                         "open":round(float(r["Open"]),2),"high":round(float(r["High"]),2),
                         "low":round(float(r["Low"]),2),"close":round(float(r["Close"]),2),
                         "volume":int(r["Volume"]) if r["Volume"] else 0}
                        for dt,r in hist.iterrows()]
                for i in range(0,len(rows),100):
                    sb.table("trade_candles").upsert(rows[i:i+100],on_conflict="symbol,date").execute()
                print(f"   ✓ {sym}: {len(rows)} candles")
            except Exception as e:
                print(f"   ⚠️  {sym}: {e}")
        print()
    except Exception as e:
        print(f"   ⚠️  Candles failed: {e}\n")


# ════════════════════════════════════════════════════════════
# SECTORAL BREADTH
# ════════════════════════════════════════════════════════════
SECTOR_INDEX_TICKERS = {
    "BANK":"^NSEBANK","IT":"^CNXIT","AUTO":"^CNXAUTO","PHARMA":"^CNXPHARMA",
    "FMCG":"^CNXFMCG","METAL":"^CNXMETAL","REALTY":"^CNXREALTY","ENERGY":"^CNXENERGY",
    "INFRA":"^CNXINFRA","MEDIA":"^CNXMEDIA","PSU_BANK":"^CNXPSUBANK","PVT_BANK":"^NIFPVTBNK",
    "OIL_GAS":"^CNXOILGAS","HEALTHCARE":"^CNXHEALTH","CONS_DUR":"^CNXCONSUMDURBL",
    "FIN_SERV":"^CNXFINANCE","DEFENCE":None,"CHEMICALS":None,"CAP_GOODS":None,"MFG":None,
}

def fetch_sector_breadth(sector_map):
    """
    Compute breadth metrics for all sectors.
    sector_map comes from fetch_sector_stocks_from_nse() — always live.
    Falls back to Supabase sector_stocks if NSE fetch failed.
    """
    print("🗺️  Computing sectoral breadth...")

    sector_map = sector_map or {}    # tolerate a failed NSE fetch (None)
    # If sector_map is empty (NSE fetch failed), load from Supabase
    if not sector_map:
        print("   ℹ️  Loading sector_stocks from Supabase (NSE unavailable)...")
        resp = sb.table("sector_stocks").select("*").execute()
        if not resp.data:
            print("   ⚠️  No sector_stocks data\n"); return
        for row in resp.data:
            sk = row["sector_key"]
            if sk not in sector_map:
                sector_map[sk] = {"name":row["sector_name"],"category":row["category"],"symbols":[]}
            sector_map[sk]["symbols"].append(row["symbol"]+".NS")

    # Nifty 500 RS baseline
    n500 = get_close_series("^CRSLDX", period="6mo")
    n500_1m = float((n500.iloc[-1]/n500.iloc[-22]  -1)*100) if len(n500)>=22  else 0
    n500_3m = float((n500.iloc[-1]/n500.iloc[-66]  -1)*100) if len(n500)>=66  else 0
    n500_6m = float((n500.iloc[-1]/n500.iloc[-126] -1)*100) if len(n500)>=126 else 0

    results = []
    for sk, meta in sector_map.items():
        syms = meta["symbols"]
        try:
            raw = yf.download(syms, period="1y", progress=False,
                              group_by="ticker", auto_adjust=True)
            a21=a50=a200=n52h=n52l=adv=dec=cnt = 0
            for sym in syms:
                try:
                    if len(syms)==1:
                        cl = raw["Close"]
                        if isinstance(cl, pd.DataFrame): cl = cl.iloc[:,0]
                    else:
                        if sym not in raw.columns.get_level_values(0): continue
                        cl = raw[sym]["Close"]
                        if isinstance(cl, pd.DataFrame): cl = cl.iloc[:,0]
                    cl = cl.dropna()
                    if len(cl) < 50: continue
                    cnt += 1
                    cur, prev = float(cl.iloc[-1]), float(cl.iloc[-2])
                    if cur > prev: adv+=1
                    elif cur < prev: dec+=1
                    if cur > float(compute_ema(cl,21).iloc[-1]):  a21+=1
                    if cur > float(compute_ema(cl,50).iloc[-1]):  a50+=1
                    if cur > float(compute_ema(cl,200).iloc[-1]): a200+=1
                    hi52=float(cl.rolling(252).max().iloc[-1])
                    lo52=float(cl.rolling(252).min().iloc[-1])
                    if cur>=hi52*0.97: n52h+=1
                    if cur<=lo52*1.03: n52l+=1
                except: continue
            if cnt==0: continue
            p21  = round(a21/cnt*100,2); p50  = round(a50/cnt*100,2)
            p200 = round(a200/cnt*100,2); p52h = round(n52h/cnt*100,2)
            p52l = round(n52l/cnt*100,2); adr  = round(adv/max(dec,1),2)
            score = round(p200*0.30+p50*0.25+p21*0.20+p52h*0.15+min(100,(adr/2)*100)*0.10,2)
            label = ("STRONG BULL" if score>=80 else "BULL" if score>=60
                     else "NEUTRAL" if score>=40 else "BEAR" if score>=20 else "STRONG BEAR")
            rs_1m=rs_3m=rs_6m=0.0; idx_level=None; idx_chg=0.0
            tk = SECTOR_INDEX_TICKERS.get(sk)
            if tk:
                try:
                    idx = get_close_series(tk, period="6mo")
                    if len(idx)>=2:
                        idx_level=round(float(idx.iloc[-1]),2)
                        idx_chg=round((float(idx.iloc[-1])/float(idx.iloc[-2])-1)*100,2)
                        s1m=(float(idx.iloc[-1])/float(idx.iloc[-22])-1)*100 if len(idx)>=22 else 0
                        s3m=(float(idx.iloc[-1])/float(idx.iloc[-66])-1)*100 if len(idx)>=66 else 0
                        s6m=(float(idx.iloc[-1])/float(idx.iloc[-126])-1)*100 if len(idx)>=126 else 0
                        rs_1m=round(s1m-n500_1m,2); rs_3m=round(s3m-n500_3m,2); rs_6m=round(s6m-n500_6m,2)
                except: pass
            results.append({
                "date":TODAY,"sector_key":sk,"sector_name":meta["name"],"category":meta["category"],
                "total_stocks":cnt,"advances":adv,"declines":dec,"unchanged":cnt-adv-dec,
                "pct_above_21ema":p21,"pct_above_50ema":p50,"pct_above_200ema":p200,
                "pct_near_52w_high":p52h,"pct_near_52w_low":p52l,"ad_ratio":adr,
                "rs_1m":rs_1m,"rs_3m":rs_3m,"rs_6m":rs_6m,
                "index_level":idx_level,"index_change_pct":idx_chg,
                "regime_score":score,"regime_label":label,
            })
            print(f"   ✓ {sk}: {cnt} stocks | Score:{score} [{label}]")
        except Exception as e:
            print(f"   ⚠️  {sk}: {e}"); continue
    if results:
        sb.table("sector_breadth").upsert(results,on_conflict="date,sector_key").execute()
        print(f"   ✓ Saved {len(results)} sectors\n")


# ════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════
# ════════════════════════════════════════════════════════════
# WATCHLIST ENRICHMENT
# Runs daily — enriches each watchlist stock with:
#   CMP, RS 1M/3M/6M vs Nifty500, EMA positions, sector score
# ════════════════════════════════════════════════════════════
# ════════════════════════════════════════════════════════════
# WATCHLIST ENRICHMENT  (updated — writes to watchlist_enriched)
# Reads from watchlist table (populated by CSV upload in Admin)
# Enriches with 21/50/200 EMA, % distances, buy range flag,
# sector name + regime score. Upserts to watchlist_enriched.
# ════════════════════════════════════════════════════════════
def fetch_fundamentals(sym_ns):
    """Fetch Market Cap, trailing PE, ROE for one NSE symbol via yfinance.
    Returns a dict with None for any field that's unavailable. Never raises."""
    out = {"market_cap": None, "pe_ratio": None, "roe": None, "pb_ratio": None}
    try:
        info = yf.Ticker(sym_ns).info or {}
        mc = info.get("marketCap")
        if mc is not None:
            out["market_cap"] = round(float(mc), 2)
        pe = info.get("trailingPE")
        if pe is not None:
            try:
                pe = float(pe)
                if pe > 0 and pe < 100000:      # guard against absurd/negative
                    out["pe_ratio"] = round(pe, 2)
            except (TypeError, ValueError):
                pass
        roe = info.get("returnOnEquity")
        if roe is not None:
            try:
                # yfinance returns ROE as a fraction (0.185) → store as % (18.50)
                out["roe"] = round(float(roe) * 100, 2)
            except (TypeError, ValueError):
                pass
        pb = info.get("priceToBook")
        if pb is not None:
            try:
                pb = float(pb)
                if pb > 0 and pb < 100000:
                    out["pb_ratio"] = round(pb, 2)
            except (TypeError, ValueError):
                pass
    except Exception as e:
        print(f"      (fundamentals unavailable for {sym_ns}: {e})")
    return out


def _union_watchlist_symbols(max_symbols=400):
    """Merge the admin `watchlist` with every user's `user_watchlist`.

    Technical data is objective and identical for everyone, so we enrich a
    single de-duplicated universe (admin curated symbols + all user symbols)
    into the shared watchlist_enriched / watchlist_candles tables.

    Returns a list of dicts: {symbol, company_name, rs_rating}.
    rs_rating is only ever set from the admin watchlist (MarketSmith CSV);
    user-added symbols carry rs_rating=None.
    """
    merged = {}  # UPPER(symbol) -> {symbol, company_name, rs_rating}

    # 1) Admin watchlist first (authoritative for company_name + rs_rating)
    try:
        wl = sb.table("watchlist").select("symbol,company_name,rs_rating").execute().data or []
    except Exception as e:
        print(f"   ⚠️  could not read watchlist: {e}")
        wl = []
    for r in wl:
        sym = (r.get("symbol") or "").strip().upper()
        if not sym:
            continue
        merged[sym] = {
            "symbol":       sym,
            "company_name": r.get("company_name"),
            "rs_rating":    r.get("rs_rating"),
        }

    # 2) User watchlists — fill in any symbols the admin list doesn't cover
    try:
        uw = sb.table("user_watchlist").select("symbol,company_name").execute().data or []
    except Exception as e:
        print(f"   ⚠️  could not read user_watchlist: {e}")
        uw = []
    for r in uw:
        sym = (r.get("symbol") or "").strip().upper()
        if not sym:
            continue
        if sym in merged:
            # keep admin company_name if present, else adopt the user's
            if not merged[sym].get("company_name") and r.get("company_name"):
                merged[sym]["company_name"] = r.get("company_name")
        else:
            merged[sym] = {
                "symbol":       sym,
                "company_name": r.get("company_name"),
                "rs_rating":    None,
            }

    rows = sorted(merged.values(), key=lambda d: d["symbol"])
    if len(rows) > max_symbols:
        print(f"   ⚠️  {len(rows)} tracked symbols exceeds cap {max_symbols}; "
              f"enriching first {max_symbols} alphabetically.")
        rows = rows[:max_symbols]
    return rows


def enrich_watchlist():
    print("📋 Enriching watchlist...")
    try:
        # Universe = admin watchlist ∪ every user's watchlist (shared technicals)
        stocks = _union_watchlist_symbols()
        if not stocks:
            print("   ⚠️  no symbols tracked yet — admin CSV or user watchlists are empty\n")
            return

        # Load sector mapping: symbol → {key, name}
        sec_resp = sb.table("sector_stocks").select("symbol,sector_key,sector_name").execute()
        sec_map  = {
            r["symbol"]: {"key": r["sector_key"], "name": r["sector_name"]}
            for r in (sec_resp.data or [])
        }

        # Load latest sector regime scores
        sb_resp = sb.table("sector_breadth") \
            .select("sector_key,regime_score,regime_label,sector_name") \
            .order("date", desc=True).limit(100).execute()
        score_map = {}
        for r in (sb_resp.data or []):
            if r["sector_key"] not in score_map:
                score_map[r["sector_key"]] = {
                    "score": r["regime_score"],
                    "label": r["regime_label"],
                    "name":  r["sector_name"],
                }

        results = []
        BATCH   = 20

        for i in range(0, len(stocks), BATCH):
            batch  = stocks[i:i+BATCH]
            syms   = [s["symbol"].strip().upper() + ".NS" for s in batch]

            try:
                raw = yf.download(
                    syms, period="1y", progress=False,
                    group_by="ticker", auto_adjust=True
                )
            except Exception as e:
                print(f"   ⚠️  Batch {i//BATCH+1} download failed: {e}")
                continue

            for stock in batch:
                sym_clean = stock["symbol"].strip().upper()
                sym_ns    = sym_clean + ".NS"
                try:
                    # Extract Close series
                    if len(syms) == 1:
                        cl = raw["Close"]
                        if isinstance(cl, pd.DataFrame): cl = cl.iloc[:, 0]
                    else:
                        if sym_ns not in raw.columns.get_level_values(0):
                            print(f"   ⚠️  {sym_clean}: not in download results")
                            continue
                        cl = raw[sym_ns]["Close"]
                        if isinstance(cl, pd.DataFrame): cl = cl.iloc[:, 0]

                    cl = cl.dropna()
                    if len(cl) < 50:
                        print(f"   ⚠️  {sym_clean}: only {len(cl)} bars — skipping")
                        continue

                    cur    = round(float(cl.iloc[-1]), 2)
                    ema21  = round(float(cl.ewm(span=21,  adjust=False).mean().iloc[-1]), 2)
                    ema50  = round(float(cl.ewm(span=50,  adjust=False).mean().iloc[-1]), 2)   # ≈ 10-week
                    ema200 = round(float(cl.ewm(span=200, adjust=False).mean().iloc[-1]), 2)

                    # % distance from each EMA
                    pct21  = round((cur - ema21)  / ema21  * 100, 2)
                    pct10w = round((cur - ema50)  / ema50  * 100, 2)   # 10-week uses 50-day
                    pct200 = round((cur - ema200) / ema200 * 100, 2)

                    # ── Buy Range Criteria (Minervini/O'Neil) ──────────────────
                    # Within 10% of 21 EMA (tight — near short-term support)
                    # Within 15% of 10-week EMA (base structure intact)
                    # Above 200 EMA but not more than 70% extended (Stage 2)
                    in_buy_range = (
                        -5   <= pct21  <= 10  and
                               pct10w  <= 15  and
                        0    <= pct200 <= 70
                    )

                    # Sector mapping
                    sec     = sec_map.get(sym_clean, {})
                    sec_key = sec.get("key")
                    sec_nm  = sec.get("name")
                    scores  = score_map.get(sec_key, {}) if sec_key else {}

                    # Fundamentals (Market Cap / PE / ROE) via yfinance .info
                    fund = fetch_fundamentals(sym_ns)

                    results.append({
                        "symbol":          sym_clean,
                        "company_name":    stock.get("company_name"),
                        "cur_price":       cur,
                        "price_change":    float(stock.get("price_change")  or 0),
                        "price_chg_pct":   float(stock.get("price_chg_pct") or 0),
                        "rs_rating":       int(stock.get("rs_rating")       or 0),
                        "ema_21":          ema21,
                        "ema_50":          ema50,
                        "ema_200":         ema200,
                        "ema_10w":         ema50,
                        "pct_from_21ema":  pct21,
                        "pct_from_10wema": pct10w,
                        "pct_from_200ema": pct200,
                        "in_buy_range":    in_buy_range,
                        "sector_key":      sec_key,
                        "sector_name":     sec_nm,
                        "sector_score":    scores.get("score"),
                        "sector_label":    scores.get("label"),
                        "market_cap":      fund["market_cap"],
                        "pe_ratio":        fund["pe_ratio"],
                        "roe":             fund["roe"],
                        "pb_ratio":        fund["pb_ratio"],
                        "enriched_date":   TODAY,
                    })

                    flag = "✅ BUY RANGE" if in_buy_range else "  —        "
                    print(
                        f"   {flag} {sym_clean:<14} "
                        f"21EMA:{pct21:+.1f}%  "
                        f"10W:{pct10w:+.1f}%  "
                        f"200EMA:{pct200:+.1f}%"
                    )

                except Exception as e:
                    print(f"   ⚠️  {sym_clean}: {e}")
                    continue

        # Upsert all results to watchlist_enriched
        if results:
            for i in range(0, len(results), 50):
                sb.table("watchlist_enriched").upsert(
                    results[i:i+50], on_conflict="symbol"
                ).execute()
            in_range = sum(1 for r in results if r["in_buy_range"])
            print(f"   ✓ {len(results)}/{len(stocks)} stocks enriched | {in_range} in buy range\n")
        else:
            print("   ⚠️  No results to save\n")

    except Exception as e:
        print(f"   ❌ Watchlist enrichment failed: {e}\n")
        import traceback; traceback.print_exc()


# ════════════════════════════════════════════════════════════
# WATCHLIST CANDLES  →  watchlist_candles
# Fetches ~24 months of daily OHLCV for every stock in the CURRENT
# watchlist and upserts into watchlist_candles. Runs in full_eod so
# the dashboard charts refresh whenever the watchlist is updated.
# The dashboard renders these with TradingView Lightweight Charts
# and computes 21/50/200 EMAs client-side from this same data.
# ════════════════════════════════════════════════════════════
def fetch_candles_for_watchlist():
    print("🕯️  Fetching 24-month candles for watchlist...")
    try:
        stocks = _union_watchlist_symbols()
        if not stocks:
            print("   ⚠️  no symbols tracked yet — nothing to chart\n")
            return

        symbols = sorted({
            s["symbol"].strip().upper()
            for s in stocks if s.get("symbol")
        })
        print(f"   {len(symbols)} symbols to fetch")

        BATCH = 20
        total = 0
        for i in range(0, len(symbols), BATCH):
            batch   = symbols[i:i+BATCH]
            syms_ns = [s + ".NS" for s in batch]
            try:
                raw = yf.download(
                    syms_ns, period="2y", progress=False,
                    group_by="ticker", auto_adjust=True
                )
            except Exception as e:
                print(f"   ⚠️  Batch {i//BATCH+1} download failed: {e}")
                continue

            rows = []
            for sym in batch:
                sym_ns = sym + ".NS"
                try:
                    if len(syms_ns) == 1:
                        df = raw
                    else:
                        if sym_ns not in raw.columns.get_level_values(0):
                            continue
                        df = raw[sym_ns]
                    df = df.dropna(subset=["Close"])
                    if df.empty:
                        continue
                    for dt, r in df.iterrows():
                        try:
                            rows.append({
                                "symbol": sym,
                                "date":   str(dt.date()),
                                "open":   round(float(r["Open"]),  2),
                                "high":   round(float(r["High"]),  2),
                                "low":    round(float(r["Low"]),   2),
                                "close":  round(float(r["Close"]), 2),
                                "volume": int(r["Volume"]) if not pd.isna(r["Volume"]) else 0,
                            })
                        except Exception:
                            continue
                except Exception as e:
                    print(f"   ⚠️  {sym}: {e}")
                    continue

            if rows:
                for j in range(0, len(rows), 500):
                    sb.table("watchlist_candles").upsert(
                        rows[j:j+500], on_conflict="symbol,date"
                    ).execute()
                total += len(rows)
                print(f"   ✓ Batch {i//BATCH+1}: {len(batch)} stocks → {len(rows)} candles")

        print(f"   ✓ {total} candles upserted across {len(symbols)} symbols\n")

    except Exception as e:
        print(f"   ❌ Watchlist candles failed: {e}\n")
        import traceback; traceback.print_exc()


if __name__ == "__main__":
    # ── Trading-day gate ──────────────────────────────────────
    # Skip dated market-data runs (intraday, FII/DII, EOD) on weekends & NSE
    # holidays, so no indicator records data for a non-trading day.
    # The pre-market BRIEF is exempt: it's news, not dated market data, and is
    # useful every day (and must be testable on weekends).
    if RUN_MODE != "premarket" and not is_trading_day():
        print(f"{'='*56}")
        print(f"  MARKET CLOSED on {TODAY} ({NOW_IST.strftime('%A')}) — "
              f"weekend or NSE holiday.")
        print(f"  Skipping pipeline; no data written.")
        print(f"{'='*56}\n")
        sys.exit(0)

    # ── Step runner: isolates each step so one failure can't abort the run ──
    # Each step runs in its own try/except. A failure is logged and recorded,
    # the pipeline continues, and existing DB data for that indicator is left
    # untouched (upserts never delete). A summary prints at the end.
    _results = []   # (step_name, "ok" | "FAILED")
    def run_step(name, fn, *args, **kwargs):
        try:
            out = fn(*args, **kwargs)
            _results.append((name, "ok"))
            return out
        except Exception as e:
            _results.append((name, "FAILED"))
            print(f"   ❌ {name} failed (continuing): {e}")
            import traceback; traceback.print_exc()
            return None

    try:
        if RUN_MODE == "intraday":
            print("⚡ INTRADAY\n")
            run_step("cmp_open_trades", fetch_cmp_for_open_trades)
            run_step("index_levels",    fetch_index_levels)

        elif RUN_MODE == "fii_dii":
            # FII/DII is now fed MANUALLY via CSV import (daily / monthly /
            # yearly) into fii_dii_activity. The auto-fetch is disabled.
            print("🏦 FII/DII is manual now (CSV-fed) — nothing to do.\n")

        elif RUN_MODE == "premarket":
            print("🌅 PRE-MARKET BRIEF\n")
            run_step("market_brief", generate_market_brief)

        else:
            print("🌙 FULL EOD\n")
            # Independent fetches first; each isolated.
            symbols = run_step("nifty500_symbols", fetch_nifty500_symbols)
            cmp_map = run_step("cmp_open_trades",  fetch_cmp_for_open_trades)
            idx     = run_step("index_levels",     fetch_index_levels)
            # index_levels returns (row, n500_val, nifty50_val); unpack safely
            n500_val   = idx[1] if idx else None
            nifty50_val= idx[2] if idx else None

            # Downstream steps — each guarded; they tolerate missing inputs
            # (params are optional / None-safe from earlier hardening).
            run_step("portfolio_snapshot", save_portfolio_snapshot, cmp_map, n500_val)
            run_step("user_stats",         compute_user_stats, cmp_map)
            run_step("market_breadth",     calculate_market_breadth, symbols, nifty50_val)
            run_step("index_heatmap",      build_index_heatmap, symbols)
            run_step("open_trade_candles", fetch_candles_for_open_trades)
            # FII/DII fed manually via CSV import — pipeline step disabled.
            # run_step("fii_dii",            fetch_fii_dii, nifty50_val)
            sector_map = run_step("sector_stocks", fetch_sector_stocks_from_nse)
            run_step("sector_breadth",     fetch_sector_breadth, sector_map)
            run_step("watchlist_enrich",   enrich_watchlist)
            run_step("watchlist_candles",  fetch_candles_for_watchlist)
            # Safety net: if the pre-market run didn't publish today's brief
            # (e.g. a missed/late scheduled run), generate it now. Never
            # overwrites an existing brief for today.
            run_step("market_brief_backfill", generate_market_brief, only_if_missing=True)

        # ── Run summary ──────────────────────────────────────
        ok     = [n for n, s in _results if s == "ok"]
        failed = [n for n, s in _results if s == "FAILED"]
        print(f"\n{'='*56}")
        print(f"  {RUN_MODE.upper()} complete — {len(ok)}/{len(_results)} steps ok")
        if failed:
            print(f"  ⚠️  FAILED: {', '.join(failed)}")
            print(f"  (existing data for failed steps left untouched; "
                  f"will refresh on next successful run)")
        print(f"{'='*56}\n")

        # Exit non-zero only if EVERYTHING failed (a real outage worth alerting on).
        # Partial success is a success — the dashboard still updates.
        if _results and len(failed) == len(_results):
            print("❌ All steps failed — flagging run as failed.")
            sys.exit(1)

    except Exception as e:
        # Safety net for anything outside the per-step guards (shouldn't happen).
        print(f"\n❌ Pipeline crashed (outside step guards): {e}")
        import traceback; traceback.print_exc()
        sys.exit(1)
