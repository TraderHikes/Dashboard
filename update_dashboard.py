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
# CORE HELPER — bullet-proof scalar extraction from yfinance
# ════════════════════════════════════════════════════════════
def get_close_series(ticker_str, period="5d"):
    """
    Download a single ticker and return a clean 1-D Close Series.
    yfinance sometimes returns a multi-level DataFrame; this handles all cases.
    """
    data = yf.download(ticker_str, period=period, progress=False, auto_adjust=True)
    if data is None or data.empty:
        return pd.Series(dtype=float)
    # Flatten multi-level columns if present
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.droplevel(1)
    close = data["Close"]
    # If still a DataFrame (shouldn't be, but safety), take first column
    if isinstance(close, pd.DataFrame):
        close = close.iloc[:, 0]
    return close.dropna()


def to_float(val):
    """Convert any scalar/Series/array to a plain Python float."""
    if val is None:
        return None
    if isinstance(val, (pd.Series, pd.DataFrame)):
        val = val.dropna()
        return float(val.iloc[-1]) if len(val) else None
    try:
        return float(val)
    except Exception:
        return None


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
                cmp = to_float(yf.Ticker(f"{sym}.NS").fast_info.last_price)
                if cmp:
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
# INDEX LEVELS  — wide-row schema
# One row per date with columns:
#   nifty50, nifty50_prev, sensex, sensex_prev, nifty500, nifty500_prev,
#   nifty_midcap, nifty_midcap_prev, nifty_it, nifty_it_prev,
#   nifty_bank, nifty_bank_prev, nifty_pharma, nifty_pharma_prev,
#   nifty_auto, nifty_auto_prev, nifty_psubank, nifty_psubank_prev,
#   nifty_metal, nifty_metal_prev, nifty_realty, nifty_realty_prev,
#   nifty_fmcg, nifty_fmcg_prev
# ════════════════════════════════════════════════════════════
INDEX_MAP = {
    "nifty50":      "^NSEI",
    "sensex":       "^BSESN",
    "nifty500":     "^CRSLDX",
    "nifty_midcap": "^NSEMDCP150",
    "nifty_it":     "^CNXIT",
    "nifty_bank":   "^NSEBANK",
    "nifty_pharma": "^CNXPHARMA",
    "nifty_auto":   "^CNXAUTO",
    "nifty_psubank":"^CNXPSUBANK",
    "nifty_metal":  "^CNXMETAL",
    "nifty_realty": "^CNXREALTY",
    "nifty_fmcg":   "^CNXFMCG",
}

def fetch_index_levels():
    print("📊 Fetching index levels...")
    row = {"snapshot_date": TODAY}
    n500_val    = None
    nifty50_val = None

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
        print(f"   ✓ Saved index_levels for {TODAY}\n")
    else:
        print("   ⚠️  No index data saved\n")
    return row, n500_val, nifty50_val


# ════════════════════════════════════════════════════════════
# PORTFOLIO SNAPSHOT
# Columns: snapshot_date, portfolio_value, total_capital,
#          cash_available, nifty500_level, cumulative_return_pct
# ════════════════════════════════════════════════════════════
def save_portfolio_snapshot(cmp_map, n500_val):
    print("📸 Saving portfolio snapshot...")
    try:
        trades   = sb.table("open_trades").select("*").execute().data or []
        deployed = 0.0
        unreal   = 0.0
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
# Exact columns your table has:
#   snapshot_date, total_stocks,
#   advancing, declining, unchanged,
#   pct_above_21ema,  above_21ema_count,
#   pct_above_50ema,  above_50ema_count,
#   pct_above_200ema, above_200ema_count,
#   new_52w_highs, new_52w_lows,
#   nifty50_close, india_vix
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
            # Flatten multi-level if needed
            if isinstance(raw.columns, pd.MultiIndex):
                pass  # keep as-is; access via raw[sym]["Close"]
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
                    cur  = float(cl.iloc[-1])
                    prev = float(cl.iloc[-2])
                    if cur > prev:   adv  += 1
                    elif cur < prev: dec  += 1
                    else:            unch += 1
                    if cur > float(compute_ema(cl, 21).iloc[-1]):  a21  += 1
                    if cur > float(compute_ema(cl, 50).iloc[-1]):  a50  += 1
                    if cur > float(compute_ema(cl, 200).iloc[-1]): a200 += 1
                    hi52 = float(cl.rolling(252).max().iloc[-1])
                    lo52 = float(cl.rolling(252).min().iloc[-1])
                    if cur >= hi52 * 0.97: h52 += 1
                    if cur <= lo52 * 1.03: l52 += 1
                except: continue
        except Exception as e:
            print(f"   ⚠️  Batch {i//BATCH+1}: {e}")

    if not valid:
        print("   ⚠️  No valid data\n"); return

    p21  = round(a21/valid*100, 2)
    p50  = round(a50/valid*100, 2)
    p200 = round(a200/valid*100, 2)

    # Fetch VIX
    india_vix = None
    try:
        vix_cl = get_close_series("^INDIAVIX", period="5d")
        if len(vix_cl): india_vix = round(float(vix_cl.iloc[-1]), 2)
    except: pass

    sb.table("market_breadth").upsert({
        "snapshot_date":    TODAY,
        "total_stocks":     valid,
        "advancing":        adv,
        "declining":        dec,
        "unchanged":        unch,
        "pct_above_21ema":  p21,  "above_21ema_count":  a21,
        "pct_above_50ema":  p50,  "above_50ema_count":  a50,
        "pct_above_200ema": p200, "above_200ema_count": a200,
        "new_52w_highs":    h52,
        "new_52w_lows":     l52,
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
        sess.get("https://www.nseindia.com",
                 headers={"User-Agent":"Mozilla/5.0"}, timeout=10)
        r = sess.get("https://www.nseindia.com/api/fiidiiTradeReact",
                     headers={"User-Agent":"Mozilla/5.0",
                               "Referer":"https://www.nseindia.com/"}, timeout=15)
        r.raise_for_status()
        fii_net = dii_net = 0.0
        for row in r.json():
            cat = str(row.get("category","")).upper()
            if "FII" in cat or "FPI" in cat:
                fii_net = float(row.get("netPurchases", row.get("netSales", 0)) or 0)
            if "DII" in cat:
                dii_net = float(row.get("netPurchases", row.get("netSales", 0)) or 0)
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
# CANDLES FOR OPEN TRADES
# ════════════════════════════════════════════════════════════
def fetch_candles_for_open_trades():
    print("🕯️  Fetching candles for open trades...")
    try:
        trades = sb.table("open_trades").select("symbol").execute().data or []
        if not trades: print("   No open trades\n"); return
        for sym in list({t["symbol"].strip().upper() for t in trades}):
            try:
                hist = yf.Ticker(f"{sym}.NS").history(
                    period="180d", interval="1d", auto_adjust=True)
                if hist.empty: continue
                rows = [{
                    "symbol": sym, "date": str(dt.date()),
                    "open":   round(float(r["Open"]),  2),
                    "high":   round(float(r["High"]),  2),
                    "low":    round(float(r["Low"]),   2),
                    "close":  round(float(r["Close"]), 2),
                    "volume": int(r["Volume"]) if r["Volume"] else 0,
                } for dt, r in hist.iterrows()]
                for i in range(0, len(rows), 100):
                    sb.table("trade_candles").upsert(
                        rows[i:i+100], on_conflict="symbol,date").execute()
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
    "BANK":        "^NSEBANK",    "IT":          "^CNXIT",
    "AUTO":        "^CNXAUTO",    "PHARMA":      "^CNXPHARMA",
    "FMCG":        "^CNXFMCG",    "METAL":       "^CNXMETAL",
    "REALTY":      "^CNXREALTY",  "ENERGY":      "^CNXENERGY",
    "INFRA":       "^CNXINFRA",   "MEDIA":       "^CNXMEDIA",
    "PSU_BANK":    "^CNXPSUBANK", "PVT_BANK":    "^NIFPVTBNK",
    "CONS_DUR":    "^CNXCONSUMDURBL",
    "HEALTHCARE":  "^CNXHEALTH",  "OIL_GAS":     "^CNXOILGAS",
    "DEFENCE":     None, "CHEMICALS": None,
    "CAP_MARKETS": None, "TEXTILES":  None,
}

def fetch_sector_breadth():
    print("🗺️  Computing sectoral breadth...")
    try:
        resp = sb.table("sector_stocks").select("*").execute()
        if not resp.data:
            print("   ⚠️  No sector_stocks data\n"); return

        sector_map = {}
        for row in resp.data:
            sk = row["sector_key"]
            if sk not in sector_map:
                sector_map[sk] = {"name": row["sector_name"],
                                  "category": row["category"], "symbols": []}
            sector_map[sk]["symbols"].append(row["symbol"] + ".NS")

        # Nifty 500 baseline for RS
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
                        if len(syms) == 1:
                            cl = raw["Close"]
                            if isinstance(cl, pd.DataFrame): cl = cl.iloc[:, 0]
                        else:
                            if sym not in raw.columns.get_level_values(0): continue
                            cl = raw[sym]["Close"]
                            if isinstance(cl, pd.DataFrame): cl = cl.iloc[:, 0]
                        cl = cl.dropna()
                        if len(cl) < 50: continue
                        cnt += 1
                        cur  = float(cl.iloc[-1])
                        prev = float(cl.iloc[-2])
                        if cur > prev: adv += 1
                        elif cur < prev: dec += 1
                        if cur > float(compute_ema(cl, 21).iloc[-1]):  a21  += 1
                        if cur > float(compute_ema(cl, 50).iloc[-1]):  a50  += 1
                        if cur > float(compute_ema(cl, 200).iloc[-1]): a200 += 1
                        hi52 = float(cl.rolling(252).max().iloc[-1])
                        lo52 = float(cl.rolling(252).min().iloc[-1])
                        if cur >= hi52*0.97: n52h += 1
                        if cur <= lo52*1.03: n52l += 1
                    except: continue
                if cnt == 0: continue

                p21  = round(a21/cnt*100, 2)
                p50  = round(a50/cnt*100, 2)
                p200 = round(a200/cnt*100, 2)
                p52h = round(n52h/cnt*100, 2)
                p52l = round(n52l/cnt*100, 2)
                adr  = round(adv/max(dec, 1), 2)
                score = round(p200*0.30 + p50*0.25 + p21*0.20 +
                              p52h*0.15 + min(100,(adr/2)*100)*0.10, 2)
                label = ("STRONG BULL" if score>=80 else "BULL" if score>=60
                         else "NEUTRAL" if score>=40 else "BEAR" if score>=20
                         else "STRONG BEAR")

                rs_1m=rs_3m=rs_6m=0.0; idx_level=None; idx_chg=0.0
                tk = SECTOR_INDEX_TICKERS.get(sk)
                if tk:
                    try:
                        idx = get_close_series(tk, period="6mo")
                        if len(idx) >= 2:
                            idx_level = round(float(idx.iloc[-1]), 2)
                            idx_chg   = round((float(idx.iloc[-1])/float(idx.iloc[-2])-1)*100, 2)
                            s1m = (float(idx.iloc[-1])/float(idx.iloc[-22]) -1)*100 if len(idx)>=22  else 0
                            s3m = (float(idx.iloc[-1])/float(idx.iloc[-66]) -1)*100 if len(idx)>=66  else 0
                            s6m = (float(idx.iloc[-1])/float(idx.iloc[-126])-1)*100 if len(idx)>=126 else 0
                            rs_1m=round(s1m-n500_1m,2)
                            rs_3m=round(s3m-n500_3m,2)
                            rs_6m=round(s6m-n500_6m,2)
                    except: pass

                results.append({
                    "date": TODAY, "sector_key": sk,
                    "sector_name": meta["name"], "category": meta["category"],
                    "total_stocks": cnt, "advances": adv, "declines": dec,
                    "unchanged": cnt-adv-dec,
                    "pct_above_21ema": p21,  "pct_above_50ema": p50,
                    "pct_above_200ema": p200, "pct_near_52w_high": p52h,
                    "pct_near_52w_low": p52l, "ad_ratio": adr,
                    "rs_1m": rs_1m, "rs_3m": rs_3m, "rs_6m": rs_6m,
                    "index_level": idx_level, "index_change_pct": idx_chg,
                    "regime_score": score, "regime_label": label,
                })
                print(f"   ✓ {sk}: {cnt} stocks | Score:{score} [{label}]")
            except Exception as e:
                print(f"   ⚠️  {sk}: {e}"); continue

        if results:
            sb.table("sector_breadth").upsert(
                results, on_conflict="date,sector_key").execute()
            print(f"   ✓ Saved {len(results)} sectors\n")
    except Exception as e:
        print(f"   ❌ Sector breadth error: {e}\n")


# ════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════
if __name__ == "__main__":
    try:
        if RUN_MODE == "intraday":
            print("⚡ INTRADAY — CMP + Index Levels\n")
            fetch_cmp_for_open_trades()
            fetch_index_levels()

        elif RUN_MODE == "fii_dii":
            print("🏦 FII/DII ONLY\n")
            fetch_fii_dii()

        else:
            print("🌙 FULL EOD — All steps\n")
            symbols = fetch_nifty500_symbols()
            cmp_map = fetch_cmp_for_open_trades()
            idx_row, n500_val, nifty50_val = fetch_index_levels()
            save_portfolio_snapshot(cmp_map, n500_val)
            calculate_market_breadth(symbols, nifty50_val)
            fetch_candles_for_open_trades()
            fetch_fii_dii()
            fetch_sector_breadth()

        print(f"\n{'='*56}\n✅  {RUN_MODE.upper()} complete\n{'='*56}\n")

    except Exception as e:
        print(f"\n❌ Pipeline crashed: {e}")
        import traceback; traceback.print_exc()
        sys.exit(1)
