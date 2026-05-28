# ════════════════════════════════════════════════════════════
# MARKET BASECAMP — Data Pipeline  (update_dashboard.py)
# ════════════════════════════════════════════════════════════

import os, sys, requests
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, date
import pytz

IST      = pytz.timezone("Asia/Kolkata")
NOW_IST  = datetime.now(IST)
TODAY    = date.today().isoformat()
HOUR_IST = NOW_IST.hour

from supabase import create_client
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
sb = create_client(SUPABASE_URL, SUPABASE_KEY)

if   HOUR_IST in [10,12,14]: RUN_MODE = "intraday"
elif HOUR_IST in [19,20]:    RUN_MODE = "fii_dii"
else:                         RUN_MODE = "full_eod"
RUN_MODE = os.environ.get("RUN_MODE", RUN_MODE) or RUN_MODE

print(f"{'='*56}\n  MARKET BASECAMP [{RUN_MODE.upper()}]")
print(f"  {NOW_IST.strftime('%d %b %Y · %I:%M %p IST')}\n{'='*56}\n")


# ════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════
def get_close_series(ticker_str, period="5d"):
    """Download a single ticker → clean 1-D Close Series."""
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
    "CAP_GOODS":   ("Capital Goods",    "midsmall", "ind_niftycapitalgoods list.csv"),
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
    "nifty500":     "^CRSLDX",   "nifty_midcap": "^NSEMDCP150",
    "nifty_it":     "^CNXIT",    "nifty_bank":   "^NSEBANK",
    "nifty_pharma": "^CNXPHARMA","nifty_auto":   "^CNXAUTO",
    "nifty_psubank":"^CNXPSUBANK","nifty_metal":  "^CNXMETAL",
    "nifty_realty": "^CNXREALTY","nifty_fmcg":   "^CNXFMCG",
}

def fetch_index_levels():
    print("📊 Fetching index levels...")
    row = {"snapshot_date": TODAY}
    n500_val = None; nifty50_val = None
    for col_key, ticker in INDEX_MAP.items():
        try:
            close = get_close_series(ticker, period="5d")
            if len(close) < 2:
                print(f"   ⚠️  {col_key}: not enough data"); continue
            current = round(float(close.iloc[-1]), 2)
            prev    = round(float(close.iloc[-2]), 2)
            row[col_key]           = current
            row[f"{col_key}_prev"] = prev
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
    try:
        trades = sb.table("open_trades").select("*").execute().data or []
        deployed = unreal = 0.0
        for t in trades:
            qty   = float(t.get("remaining_qty") or t.get("quantity") or 0)
            entry = float(t.get("avg_entry_price") or t.get("entry_price") or 0)
            cmp   = float(cmp_map.get(t["symbol"].upper(), t.get("cmp") or entry))
            deployed += entry * qty
            unreal   += (cmp - entry) * qty
        total_cap  = float(os.environ.get("TOTAL_CAPITAL", 2500000))
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
    print(f"🔬 Computing market breadth ({len(symbols)} stocks)...")
    a21=a50=a200=h52=l52=adv=dec=unch=valid = 0
    BATCH = 50
    for i in range(0, len(symbols), BATCH):
        batch = symbols[i:i+BATCH]
        try:
            raw = yf.download(batch, period="1y", progress=False,
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
                    hi52 = float(cl.rolling(252).max().iloc[-1])
                    lo52 = float(cl.rolling(252).min().iloc[-1])
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
def fetch_fii_dii():
    print("🏦 Fetching FII/DII data...")
    try:
        sess = requests.Session()
        sess.get("https://www.nseindia.com", headers={"User-Agent":"Mozilla/5.0"}, timeout=10)
        r = sess.get("https://www.nseindia.com/api/fiidiiTradeReact",
                     headers={"User-Agent":"Mozilla/5.0",
                               "Referer":"https://www.nseindia.com/"}, timeout=15)
        r.raise_for_status()
        fii_net = dii_net = 0.0
        for row in r.json():
            cat = str(row.get("category","")).upper()
            if "FII" in cat or "FPI" in cat:
                fii_net = float(row.get("netPurchases", row.get("netSales",0)) or 0)
            if "DII" in cat:
                dii_net = float(row.get("netPurchases", row.get("netSales",0)) or 0)
        sb.table("fii_dii_activity").upsert({
            "activity_date": TODAY,
            "fii_cash_net":  fii_net,
            "dii_cash_net":  dii_net,
            "source":        "NSE"
        }, on_conflict="activity_date").execute()
        print(f"   ✓ FII: ₹{fii_net:,.2f}Cr | DII: ₹{dii_net:,.2f}Cr\n")
    except Exception as e:
        print(f"   ⚠️  FII/DII failed: {e}\n")


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
# ══════════════════════════════════════════════════════
# WATCHLIST ENRICHMENT — add to update_dashboard.py
# Called in full_eod run after sector breadth
# ══════════════════════════════════════════════════════

def enrich_watchlist():
    """
    For every stock in watchlist table:
    1. Fetch 1yr daily candles via yfinance
    2. Compute 21 EMA, 50 EMA (≈10w), 200 EMA
    3. Compute % distance from each EMA
    4. Flag in_buy_range:
         price <= 10% above 21 EMA  AND
         price <= 15% above 10w EMA AND
         price <= 70% above 200 EMA
    5. Map sector from sector_stocks
    6. Pull sector_score from latest sector_breadth
    7. Upsert into watchlist_enriched
    """
    print("📋 Enriching watchlist...")

    # Load watchlist symbols
    resp = sb.table("watchlist").select("*").execute()
    stocks = resp.data or []
    if not stocks:
        print("   ⚠️  watchlist table is empty. Upload CSV via Admin Panel.\n")
        return

    # Load sector mapping (symbol → sector_key)
    sec_resp = sb.table("sector_stocks").select("symbol,sector_key,sector_name").execute()
    sec_map  = {r["symbol"]: {"key": r["sector_key"], "name": r["sector_name"]}
                for r in (sec_resp.data or [])}

    # Load latest sector scores
    sb_resp = sb.table("sector_breadth").select(
        "sector_key,regime_score,regime_label,sector_name"
    ).order("date", desc=True).limit(100).execute()
    score_map = {}
    seen_sectors = set()
    for r in (sb_resp.data or []):
        if r["sector_key"] not in seen_sectors:
            score_map[r["sector_key"]] = {
                "score": r["regime_score"],
                "label": r["regime_label"],
                "name":  r["sector_name"],
            }
            seen_sectors.add(r["sector_key"])

    results = []
    BATCH = 20

    for i in range(0, len(stocks), BATCH):
        batch = stocks[i:i+BATCH]
        syms  = [s["symbol"].strip().upper() + ".NS" for s in batch]
        sym_map = {s["symbol"].strip().upper(): s for s in batch}

        try:
            raw = yf.download(syms, period="1y", progress=False,
                              group_by="ticker", auto_adjust=True)
        except Exception as e:
            print(f"   ⚠️  Batch {i//BATCH+1} download failed: {e}")
            continue

        for stock in batch:
            sym_clean = stock["symbol"].strip().upper()
            sym_ns    = sym_clean + ".NS"
            try:
                # Extract close series
                if len(syms) == 1:
                    cl = raw["Close"]
                    if isinstance(cl, pd.DataFrame): cl = cl.iloc[:,0]
                else:
                    if sym_ns not in raw.columns.get_level_values(0): continue
                    cl = raw[sym_ns]["Close"]
                    if isinstance(cl, pd.DataFrame): cl = cl.iloc[:,0]
                cl = cl.dropna()
                if len(cl) < 50:
                    print(f"   ⚠️  {sym_clean}: insufficient data ({len(cl)} bars)")
                    continue

                cur = float(cl.iloc[-1])

                # Compute EMAs
                ema21  = float(cl.ewm(span=21,  adjust=False).mean().iloc[-1])
                ema50  = float(cl.ewm(span=50,  adjust=False).mean().iloc[-1])
                ema200 = float(cl.ewm(span=200, adjust=False).mean().iloc[-1]) if len(cl)>=200 else float(cl.ewm(span=200, adjust=False).mean().iloc[-1])
                ema10w = ema50  # 10-week ≈ 50-day

                # % distance from each EMA (positive = above, negative = below)
                pct21  = round((cur - ema21)  / ema21  * 100, 2)
                pct10w = round((cur - ema10w) / ema10w * 100, 2)
                pct200 = round((cur - ema200) / ema200 * 100, 2)

                # Buy range criteria (Minervini VCP / Stage 2 proximity)
                # Within 10% of 21 EMA (tight consolidation near support)
                # Within 15% of 10-week EMA (base structure)
                # Within 70% of 200 EMA (above long-term trend, not extended)
                in_buy_range = (
                    -5 <= pct21  <= 10  and   # not more than 5% below or 10% above 21 EMA
                    pct10w       <= 15  and   # within 15% of 10-week
                    0  <= pct200 <= 70        # above 200 EMA but not more than 70% extended
                )

                # Sector mapping
                sec     = sec_map.get(sym_clean, {})
                sec_key = sec.get("key")
                sec_nm  = sec.get("name")
                scores  = score_map.get(sec_key, {}) if sec_key else {}

                # Use CMP from yfinance if not available in CSV
                latest_price = float(stock.get("cur_price") or cur)
                latest_chg   = float(stock.get("price_change") or 0)
                latest_chgpct= float(stock.get("price_chg_pct") or 0)

                results.append({
                    "symbol":          sym_clean,
                    "company_name":    stock.get("company_name"),
                    "cur_price":       round(cur, 2),       # always use latest yf price
                    "price_change":    round(latest_chg, 2),
                    "price_chg_pct":   round(latest_chgpct, 2),
                    "rs_rating":       int(stock.get("rs_rating") or 0),
                    "ema_21":          round(ema21, 2),
                    "ema_50":          round(ema50, 2),
                    "ema_200":         round(ema200, 2),
                    "ema_10w":         round(ema10w, 2),
                    "pct_from_21ema":  pct21,
                    "pct_from_10wema": pct10w,
                    "pct_from_200ema": pct200,
                    "in_buy_range":    in_buy_range,
                    "sector_key":      sec_key,
                    "sector_name":     sec_nm,
                    "sector_score":    scores.get("score"),
                    "sector_label":    scores.get("label"),
                    "enriched_date":   TODAY,
                })

                status = "✅ BUY RANGE" if in_buy_range else "  —"
                print(f"   {status} {sym_clean}: 21EMA {pct21:+.1f}% | 10w {pct10w:+.1f}% | 200d {pct200:+.1f}%")

            except Exception as e:
                print(f"   ⚠️  {sym_clean}: {e}")
                continue

    if results:
        # Upsert in batches
        for i in range(0, len(results), 50):
            sb.table("watchlist_enriched").upsert(
                results[i:i+50], on_conflict="symbol"
            ).execute()
        in_range = sum(1 for r in results if r["in_buy_range"])
        print(f"   ✓ {len(results)} stocks enriched | {in_range} in buy range\n")
    else:
        print("   ⚠️  No results to save\n")
