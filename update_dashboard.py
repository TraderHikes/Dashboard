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
            # Above-EMA flags — only for indices shown in the Key Indices
            # table (those have the DB columns). None when not enough history.
            if col_key in EMA_FLAG_INDICES:
                n = len(close)
                for p in (10, 21, 50):
                    if n >= p:
                        ema_val = float(compute_ema(close, p).iloc[-1])
                        row[f"{col_key}_above_{p}ema"] = bool(current > ema_val)
                    else:
                        row[f"{col_key}_above_{p}ema"] = None
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
    print("📸 Saving portfolio snapshot...")
    cmp_map = cmp_map or {}          # tolerate a failed cmp fetch (None)
    try:
        trades = sb.table("open_trades").select("*").execute().data or []
        deployed = unreal = 0.0
        for t in trades:
            qty   = float(t.get("remaining_qty") or t.get("quantity") or 0)
            entry = float(t.get("avg_entry_price") or t.get("entry_price") or 0)
            cmp   = float(cmp_map.get(t["symbol"].upper(), t.get("cmp") or entry))
            deployed += entry * qty
            unreal   += (cmp - entry) * qty
        total_cap  = float(os.environ.get("TOTAL_CAPITAL", 8500000))
        cash_avail = max(0.0, total_cap - deployed)
        port_val   = total_cap + unreal
        cum_ret    = round((port_val / total_cap - 1) * 100, 4) if total_cap else 0.0
        sb.table("portfolio_snapshots").upsert({
            "snapshot_date":        TODAY,
            "portfolio_value":       round(port_val, 2),
            "total_capital":         round(total_cap, 2),
            "cash_available":        round(cash_avail, 2),
            "nifty500_level":        n500_val,
            "cumulative_return_pct": cum_ret,
        }, on_conflict="snapshot_date").execute()
        print(f"   ✓ Portfolio ₹{port_val:,.0f} | cash ₹{cash_avail:,.0f}\n")
    except Exception as e:
        print(f"   ⚠️  Snapshot failed: {e}\n")


# ════════════════════════════════════════════════════════════
# MARKET BREADTH
# ════════════════════════════════════════════════════════════
def calculate_market_breadth(symbols, nifty50_close=None):
    if not symbols:
        print("   ⚠️  No symbols (upstream fetch failed) — skipping breadth.")
        return
    print(f"🔬 Computing market breadth ({len(symbols)} stocks)...")
    a21=a50=a200=h52=l52=adv=dec=unch=valid = 0
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
    sb.table("market_breadth").upsert({
        "snapshot_date":    TODAY,
        "total_stocks":     valid,
        "advancing":        adv, "declining": dec, "unchanged": unch,
        "pct_above_21ema":  p21,  "above_21ema_count":  a21,
        "pct_above_50ema":  p50,  "above_50ema_count":  a50,
        "pct_above_200ema": p200, "above_200ema_count": a200,
        "new_52w_highs":    h52,  "new_52w_lows":       l52,
        "nifty50_close":    nifty50_close,
        "india_vix":        india_vix,
    }, on_conflict="snapshot_date").execute()
    print(f"   ✓ {valid} stocks | 21:{p21}% 50:{p50}% 200:{p200}% | VIX:{india_vix}\n")


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

    fii_net = dii_net = None
    for row in data:
        cat = str(row.get("category", "")).upper()
        # netPurchases can be a number or string; netSales is the fallback key
        raw_val = row.get("netPurchases", row.get("netSales", None))
        try:
            val = float(str(raw_val).replace(",", "")) if raw_val not in (None, "", "-") else None
        except (TypeError, ValueError):
            val = None
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


def enrich_watchlist():
    print("📋 Enriching watchlist...")
    try:
        # Load raw watchlist (uploaded from MarketSmith CSV)
        resp = sb.table("watchlist").select("*").execute()
        stocks = resp.data or []
        if not stocks:
            print("   ⚠️  watchlist table is empty — upload CSV via Admin Panel\n")
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
        resp = sb.table("watchlist").select("symbol").execute()
        stocks = resp.data or []
        if not stocks:
            print("   ⚠️  watchlist table is empty — nothing to chart\n")
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
    # Skip ALL runs (intraday, FII/DII, EOD) on weekends & NSE holidays,
    # so no indicator records data for a non-trading day.
    if not is_trading_day():
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
            print("🏦 FII/DII ONLY\n")
            run_step("fii_dii", fetch_fii_dii)

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
            run_step("market_breadth",     calculate_market_breadth, symbols, nifty50_val)
            run_step("open_trade_candles", fetch_candles_for_open_trades)
            run_step("fii_dii",            fetch_fii_dii, nifty50_val)
            sector_map = run_step("sector_stocks", fetch_sector_stocks_from_nse)
            run_step("sector_breadth",     fetch_sector_breadth, sector_map)
            run_step("watchlist_enrich",   enrich_watchlist)
            run_step("watchlist_candles",  fetch_candles_for_watchlist)

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
