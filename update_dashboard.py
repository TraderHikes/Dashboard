# ════════════════════════════════════════════════════════════
# MARKET BASECAMP — Data Pipeline
# update_dashboard.py
# Runs via GitHub Actions on schedule + manual trigger
# ════════════════════════════════════════════════════════════

import os, sys, time, json, requests
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from supabase import create_client

import pytz
IST = pytz.timezone("Asia/Kolkata")
NOW_IST   = datetime.now(IST)
TODAY_STR = date.today().isoformat()

# ── Supabase ────────────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
sb = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── Run mode (set by GitHub Actions schedule) ────────────────
HOUR_IST = NOW_IST.hour
if   HOUR_IST in [10, 12, 14]: RUN_MODE = "intraday"
elif HOUR_IST in [19, 20]:     RUN_MODE = "fii_dii"
else:                           RUN_MODE = "full_eod"
# Allow manual override via env var
RUN_MODE = os.environ.get("RUN_MODE", RUN_MODE) or RUN_MODE

print(f"{'='*58}")
print(f"  MARKET BASECAMP — Pipeline [{RUN_MODE.upper()}]")
print(f"  {NOW_IST.strftime('%d %b %Y · %I:%M %p IST')}")
print(f"{'='*58}\n")


# ════════════════════════════════════════════════════════════
# NIFTY 500 SYMBOLS
# ════════════════════════════════════════════════════════════
def fetch_nifty500_symbols():
    print("📋 Fetching Nifty 500 symbols...")
    try:
        url = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "text/csv"}
        r = requests.get(url, headers=headers, timeout=20)
        r.raise_for_status()
        df = pd.read_csv(pd.io.common.BytesIO(r.content))
        col = [c for c in df.columns if "symbol" in c.lower()]
        if not col: raise ValueError("Symbol column not found")
        syms = [s.strip() + ".NS" for s in df[col[0]].dropna().tolist()]
        print(f"   ✓ {len(syms)} symbols loaded\n")
        return syms
    except Exception as e:
        print(f"   ⚠️  NSE fetch failed ({e}), using fallback list\n")
        return [s + ".NS" for s in [
            "RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK","HINDUNILVR","ITC",
            "SBIN","BAJFINANCE","KOTAKBANK","AXISBANK","LT","ASIANPAINT","MARUTI",
            "TITAN","SUNPHARMA","ULTRACEMCO","WIPRO","HCLTECH","ONGC",
            "NTPC","POWERGRID","JSWSTEEL","TATASTEEL","ADANIPORTS","BAJAJ-AUTO",
            "TECHM","EICHERMOT","DRREDDY","DIVISLAB","CIPLA","M&M","TATAMOTORS",
            "NESTLEIND","BRITANNIA","DABUR","MARICO","COALINDIA","HINDALCO","VEDL"
        ]]


# ════════════════════════════════════════════════════════════
# CMP for Open Trades
# ════════════════════════════════════════════════════════════
def fetch_cmp_for_open_trades():
    print("💹 Updating CMP for open trades...")
    try:
        trades = sb.table("open_trades").select("symbol").execute().data
        if not trades:
            print("   No open trades found.\n"); return {}

        symbols = list({t["symbol"].strip().upper() for t in trades})
        cmp_map = {}
        for sym in symbols:
            try:
                tk = yf.Ticker(f"{sym}.NS")
                info = tk.fast_info
                cmp = round(float(info.last_price), 2)
                sb.table("open_trades").update({
                    "cmp": cmp,
                    "updated_at": NOW_IST.isoformat()
                }).eq("symbol", sym).execute()
                cmp_map[sym] = cmp
                print(f"   ✓ {sym}: ₹{cmp}")
            except Exception as e:
                print(f"   ⚠️  {sym}: {e}")
        print()
        return cmp_map
    except Exception as e:
        print(f"   ❌ {e}\n"); return {}


# ════════════════════════════════════════════════════════════
# Index Levels (12 NSE Indices)
# ════════════════════════════════════════════════════════════
INDEX_TICKERS = {
    "NIFTY 50":       "^NSEI",
    "NIFTY 500":      "^CRSLDX",
    "BANK NIFTY":     "^NSEBANK",
    "MIDCAP 150":     "^NSEMDCP150",
    "SMALLCAP 250":   "^NSESC250",
    "NIFTY IT":       "^CNXIT",
    "NIFTY AUTO":     "^CNXAUTO",
    "NIFTY PHARMA":   "^CNXPHARMA",
    "NIFTY FMCG":     "^CNXFMCG",
    "NIFTY METAL":    "^CNXMETAL",
    "NIFTY REALTY":   "^CNXREALTY",
    "NIFTY ENERGY":   "^CNXENERGY",
}

def fetch_index_levels():
    print("📊 Fetching index levels...")
    results = {}
    for name, ticker in INDEX_TICKERS.items():
        try:
            data = yf.download(ticker, period="5d", progress=False, auto_adjust=True)
            if data.empty or len(data) < 2: continue
            current = round(float(data["Close"].iloc[-1]), 2)
            prev    = round(float(data["Close"].iloc[-2]), 2)
            chg_pct = round((current / prev - 1) * 100, 2)
            sb.table("index_levels").upsert({
                "date":       TODAY_STR,
                "index_name": name,
                "level":      current,
                "prev_close": prev,
                "change_pct": chg_pct,
                "updated_at": NOW_IST.isoformat()
            }, on_conflict="date,index_name").execute()
            results[name] = {"level": current, "prev": prev, "chg": chg_pct}
            print(f"   ✓ {name}: {current:,.2f} ({chg_pct:+.2f}%)")
        except Exception as e:
            print(f"   ⚠️  {name}: {e}")
    print()
    return results


# ════════════════════════════════════════════════════════════
# Portfolio Snapshot
# ════════════════════════════════════════════════════════════
def save_portfolio_snapshot(cmp_map, index_levels):
    print("📸 Saving portfolio snapshot...")
    try:
        trades = sb.table("open_trades").select("*").execute().data
        deployed = 0
        unrealised = 0
        for t in trades:
            sym = t["symbol"].upper()
            qty = t.get("quantity", 0) or 0
            entry = float(t.get("avg_entry_price") or t.get("entry_price") or 0)
            cmp   = float(cmp_map.get(sym, t.get("cmp", entry)) or entry)
            deployed   += entry * qty
            unrealised += (cmp - entry) * qty

        n500_level = None
        if "NIFTY 500" in index_levels:
            n500_level = index_levels["NIFTY 500"]["level"]

        closed = sb.table("closed_trades").select("realised_pnl").execute().data
        realised = sum(float(r.get("realised_pnl") or 0) for r in closed)

        sb.table("portfolio_snapshots").upsert({
            "date":                  TODAY_STR,
            "deployed_capital":      round(deployed, 2),
            "unrealised_pnl":        round(unrealised, 2),
            "realised_pnl":          round(realised, 2),
            "nifty500_level":        n500_level,
            "updated_at":            NOW_IST.isoformat()
        }, on_conflict="date").execute()
        print(f"   ✓ Snapshot saved — deployed: ₹{deployed:,.0f}, unrealised P&L: ₹{unrealised:,.0f}\n")
    except Exception as e:
        print(f"   ⚠️  Snapshot failed: {e}\n")


# ════════════════════════════════════════════════════════════
# Market Breadth (Nifty 500)
# ════════════════════════════════════════════════════════════
def compute_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def calculate_market_breadth(symbols, index_levels):
    print(f"🔬 Computing market breadth ({len(symbols)} stocks)...")
    above_21 = above_50 = above_200 = 0
    near_52h = near_52l = 0
    advances = declines = unchanged = 0
    valid = 0

    batch_size = 50
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        try:
            raw = yf.download(batch, period="1y", progress=False,
                              group_by="ticker", auto_adjust=True)
            for sym in batch:
                try:
                    close = raw[sym]["Close"] if len(batch) > 1 else raw["Close"]
                    close = close.dropna()
                    if len(close) < 50: continue
                    valid += 1
                    cur  = close.iloc[-1]
                    prev = close.iloc[-2]
                    if cur > prev:   advances += 1
                    elif cur < prev: declines += 1
                    else:            unchanged += 1
                    if cur > compute_ema(close, 21).iloc[-1]:  above_21  += 1
                    if cur > compute_ema(close, 50).iloc[-1]:  above_50  += 1
                    if cur > compute_ema(close, 200).iloc[-1]: above_200 += 1
                    h52 = close.rolling(252).max().iloc[-1]
                    l52 = close.rolling(252).min().iloc[-1]
                    if cur >= h52 * 0.97: near_52h += 1
                    if cur <= l52 * 1.03: near_52l += 1
                except: continue
        except Exception as e:
            print(f"   ⚠️  Batch {i//batch_size+1} error: {e}")

    if valid == 0:
        print("   ⚠️  No valid data. Skipping breadth save.\n"); return

    p21  = round(above_21  / valid * 100, 2)
    p50  = round(above_50  / valid * 100, 2)
    p200 = round(above_200 / valid * 100, 2)
    p52h = round(near_52h  / valid * 100, 2)
    p52l = round(near_52l  / valid * 100, 2)
    adr  = round(advances / max(declines, 1), 2)

    n50_chg = index_levels.get("NIFTY 50", {}).get("chg", 0)
    regime_score = round(p200*0.30 + p50*0.25 + p21*0.20 + p52h*0.15 + min(100,(adr/2)*100)*0.10, 2)
    if regime_score >= 80:   rl = "STRONG BULL"
    elif regime_score >= 60: rl = "BULL"
    elif regime_score >= 40: rl = "NEUTRAL"
    elif regime_score >= 20: rl = "BEAR"
    else:                    rl = "STRONG BEAR"

    sb.table("market_breadth").upsert({
        "date":              TODAY_STR,
        "total_stocks":      valid,
        "above_21ema":       above_21,  "pct_above_21ema":  p21,
        "above_50ema":       above_50,  "pct_above_50ema":  p50,
        "above_200ema":      above_200, "pct_above_200ema": p200,
        "near_52w_high":     near_52h,  "pct_near_52w_high":p52h,
        "near_52w_low":      near_52l,  "pct_near_52w_low": p52l,
        "advances":          advances,  "declines": declines, "unchanged": unchanged,
        "ad_ratio":          adr,
        "nifty50_change_pct":n50_chg,
        "regime_score":      regime_score,
        "regime_label":      rl,
        "updated_at":        NOW_IST.isoformat()
    }, on_conflict="date").execute()

    print(f"   ✓ Breadth: {valid} stocks | 21:{p21}% 50:{p50}% 200:{p200}% | Score:{regime_score} [{rl}]\n")


# ════════════════════════════════════════════════════════════
# FII / DII Activity
# ════════════════════════════════════════════════════════════
def fetch_fii_dii():
    print("🏦 Fetching FII/DII data...")
    try:
        url = "https://www.nseindia.com/api/fiidiiTradeReact"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept":     "application/json",
            "Referer":    "https://www.nseindia.com/",
        }
        sess = requests.Session()
        sess.get("https://www.nseindia.com", headers=headers, timeout=10)
        r = sess.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        data = r.json()
        fii_net = dii_net = 0
        for row in data:
            cat = str(row.get("category","")).upper()
            if "FII" in cat or "FPI" in cat:
                fii_net = float(row.get("netPurchases", row.get("netSales", 0)) or 0)
            if "DII" in cat:
                dii_net = float(row.get("netPurchases", row.get("netSales", 0)) or 0)
        sb.table("fii_dii_activity").upsert({
            "activity_date": TODAY_STR,
            "fii_cash_net":  fii_net,
            "dii_cash_net":  dii_net,
            "source":        "NSE",
            "updated_at":    NOW_IST.isoformat()
        }, on_conflict="activity_date").execute()
        print(f"   ✓ FII: ₹{fii_net:,.2f} Cr | DII: ₹{dii_net:,.2f} Cr\n")
    except Exception as e:
        print(f"   ⚠️  NSE failed ({e}), trying BSE...")
        try:
            url2 = "https://api.bseindia.com/BseIndiaAPI/api/FIIDIIData/w"
            r2 = requests.get(url2, headers={"User-Agent":"Mozilla/5.0","Referer":"https://www.bseindia.com/"}, timeout=15)
            d2 = r2.json()
            row2 = d2[0] if isinstance(d2, list) else d2
            fii_net = float(row2.get("FII", row2.get("fiiNet", 0)) or 0)
            dii_net = float(row2.get("DII", row2.get("diiNet", 0)) or 0)
            sb.table("fii_dii_activity").upsert({
                "activity_date": TODAY_STR,
                "fii_cash_net":  fii_net,
                "dii_cash_net":  dii_net,
                "source":        "BSE",
                "updated_at":    NOW_IST.isoformat()
            }, on_conflict="activity_date").execute()
            print(f"   ✓ BSE fallback: FII ₹{fii_net:,.2f} Cr | DII ₹{dii_net:,.2f} Cr\n")
        except Exception as e2:
            print(f"   ⚠️  Both failed: {e2}\n")


# ════════════════════════════════════════════════════════════
# Candles for Open Trades (TradingView charts)
# ════════════════════════════════════════════════════════════
def fetch_candles_for_open_trades():
    print("🕯️  Fetching candles for open trades...")
    try:
        trades = sb.table("open_trades").select("symbol").execute().data
        if not trades: print("   No open trades.\n"); return
        symbols = list({t["symbol"].strip().upper() for t in trades})
        for sym in symbols:
            try:
                ticker = yf.Ticker(f"{sym}.NS")
                hist   = ticker.history(period="180d", interval="1d", auto_adjust=True)
                if hist.empty: continue
                rows = []
                for dt, row in hist.iterrows():
                    rows.append({
                        "symbol": sym,
                        "date":   dt.strftime("%Y-%m-%d"),
                        "open":   round(float(row["Open"]),  2),
                        "high":   round(float(row["High"]),  2),
                        "low":    round(float(row["Low"]),   2),
                        "close":  round(float(row["Close"]), 2),
                        "volume": int(row["Volume"]) if row["Volume"] else 0,
                    })
                for i in range(0, len(rows), 100):
                    sb.table("trade_candles").upsert(rows[i:i+100], on_conflict="symbol,date").execute()
                print(f"   ✓ {sym}: {len(rows)} candles")
            except Exception as e:
                print(f"   ⚠️  {sym}: {e}")
        print()
    except Exception as e:
        print(f"   ⚠️  Candles failed: {e}\n")


# ════════════════════════════════════════════════════════════
# SECTORAL BREADTH  ← NEW
# ════════════════════════════════════════════════════════════
SECTOR_INDEX_TICKERS = {
    "BANK":        "^NSEBANK",
    "IT":          "^CNXIT",
    "AUTO":        "^CNXAUTO",
    "PHARMA":      "^CNXPHARMA",
    "FMCG":        "^CNXFMCG",
    "METAL":       "^CNXMETAL",
    "REALTY":      "^CNXREALTY",
    "ENERGY":      "^CNXENERGY",
    "INFRA":       "^CNXINFRA",
    "MEDIA":       "^CNXMEDIA",
    "PSU_BANK":    "^CNXPSUBANK",
    "PVT_BANK":    "^NIFPVTBNK",
    "CONS_DUR":    "^CNXCONSUMDURBL",
    "HEALTHCARE":  "^CNXHEALTH",
    "OIL_GAS":     "^CNXOILGAS",
    "DEFENCE":     None,
    "CHEMICALS":   None,
    "CAP_MARKETS": None,
    "TEXTILES":    None,
}

def fetch_sector_breadth():
    print("🗺️  Computing sectoral breadth (19 sectors)...")
    try:
        # Load sector→stock mapping from Supabase
        resp = sb.table("sector_stocks").select("*").execute()
        sector_map = {}
        for row in resp.data:
            sk = row["sector_key"]
            if sk not in sector_map:
                sector_map[sk] = {"name": row["sector_name"], "category": row["category"], "symbols": []}
            sector_map[sk]["symbols"].append(row["symbol"] + ".NS")

        if not sector_map:
            print("   ⚠️  No sector_stocks data. Run sector_schema.sql first.\n"); return

        # Nifty 500 baseline for RS
        n500 = yf.download("^CRSLDX", period="6mo", progress=False, auto_adjust=True)["Close"].dropna()
        n500_1m  = float((n500.iloc[-1]/n500.iloc[-22]  -1)*100) if len(n500)>=22  else 0
        n500_3m  = float((n500.iloc[-1]/n500.iloc[-66]  -1)*100) if len(n500)>=66  else 0
        n500_6m  = float((n500.iloc[-1]/n500.iloc[-126] -1)*100) if len(n500)>=126 else 0

        results = []
        for sk, meta in sector_map.items():
            symbols = meta["symbols"]
            try:
                raw = yf.download(symbols, period="1y", progress=False,
                                  group_by="ticker", auto_adjust=True)
                a21=a50=a200=n52h=n52l=adv=dec=valid_count=0

                for sym in symbols:
                    try:
                        close = raw[sym]["Close"] if len(symbols)>1 else raw["Close"]
                        close = close.dropna()
                        if len(close) < 50: continue
                        valid_count += 1
                        cur = close.iloc[-1]; prev = close.iloc[-2]
                        if cur > prev: adv += 1
                        elif cur < prev: dec += 1
                        if cur > compute_ema(close,21).iloc[-1]:  a21  += 1
                        if cur > compute_ema(close,50).iloc[-1]:  a50  += 1
                        if cur > compute_ema(close,200).iloc[-1]: a200 += 1
                        h52 = close.rolling(252).max().iloc[-1]
                        l52 = close.rolling(252).min().iloc[-1]
                        if cur >= h52*0.97: n52h += 1
                        if cur <= l52*1.03: n52l += 1
                    except: continue

                if valid_count == 0: continue

                p21  = round(a21/valid_count*100, 2)
                p50  = round(a50/valid_count*100, 2)
                p200 = round(a200/valid_count*100, 2)
                p52h = round(n52h/valid_count*100, 2)
                p52l = round(n52l/valid_count*100, 2)
                adr  = round(adv/max(dec,1), 2)
                ad_score = min(100, (adr/2)*100)
                score = round(p200*0.30 + p50*0.25 + p21*0.20 + p52h*0.15 + ad_score*0.10, 2)
                if score >= 80:   rl = "STRONG BULL"
                elif score >= 60: rl = "BULL"
                elif score >= 40: rl = "NEUTRAL"
                elif score >= 20: rl = "BEAR"
                else:             rl = "STRONG BEAR"

                # Sector index RS
                rs_1m = rs_3m = rs_6m = 0.0
                idx_level = None; idx_chg = 0.0
                idx_tk = SECTOR_INDEX_TICKERS.get(sk)
                if idx_tk:
                    try:
                        idx_data = yf.download(idx_tk, period="6mo", progress=False, auto_adjust=True)["Close"].dropna()
                        if len(idx_data) >= 2:
                            idx_level = round(float(idx_data.iloc[-1]), 2)
                            idx_chg   = round((float(idx_data.iloc[-1])/float(idx_data.iloc[-2])-1)*100, 2)
                            s1m = (float(idx_data.iloc[-1])/float(idx_data.iloc[-22])  -1)*100 if len(idx_data)>=22  else 0
                            s3m = (float(idx_data.iloc[-1])/float(idx_data.iloc[-66])  -1)*100 if len(idx_data)>=66  else 0
                            s6m = (float(idx_data.iloc[-1])/float(idx_data.iloc[-126]) -1)*100 if len(idx_data)>=126 else 0
                            rs_1m = round(s1m - n500_1m, 2)
                            rs_3m = round(s3m - n500_3m, 2)
                            rs_6m = round(s6m - n500_6m, 2)
                    except: pass

                results.append({
                    "date": TODAY_STR, "sector_key": sk,
                    "sector_name": meta["name"], "category": meta["category"],
                    "total_stocks": valid_count, "advances": adv, "declines": dec,
                    "unchanged": valid_count-adv-dec,
                    "pct_above_21ema": p21, "pct_above_50ema": p50, "pct_above_200ema": p200,
                    "pct_near_52w_high": p52h, "pct_near_52w_low": p52l,
                    "ad_ratio": adr, "rs_1m": rs_1m, "rs_3m": rs_3m, "rs_6m": rs_6m,
                    "index_level": idx_level, "index_change_pct": idx_chg,
                    "regime_score": score, "regime_label": rl,
                    "updated_at": NOW_IST.isoformat()
                })
                print(f"   ✓ {sk}: {valid_count} stocks | Score:{score} [{rl}]")
            except Exception as e:
                print(f"   ⚠️  {sk}: {e}"); continue

        if results:
            sb.table("sector_breadth").upsert(results, on_conflict="date,sector_key").execute()
            print(f"   ✓ Saved {len(results)} sectors\n")
        else:
            print("   ⚠️  No sector results saved\n")
    except Exception as e:
        print(f"   ❌ Sector breadth failed: {e}\n")


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
            print("🏦 FII/DII MODE\n")
            fetch_fii_dii()

        else:
            print("🌙 FULL EOD — All steps\n")
            symbols = fetch_nifty500_symbols()
            cmp_map = fetch_cmp_for_open_trades()
            index_levels = fetch_index_levels()
            save_portfolio_snapshot(cmp_map, index_levels)
            calculate_market_breadth(symbols, index_levels)
            fetch_candles_for_open_trades()
            fetch_fii_dii()
            fetch_sector_breadth()        # ← runs at end of full EOD

        print("="*58)
        print(f"✅ {RUN_MODE.upper()} complete!")
        print(f"   {NOW_IST.strftime('%d %b %Y · %I:%M %p IST')}")
        print("="*58 + "\n")

    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        import traceback; traceback.print_exc()
        sys.exit(1)
