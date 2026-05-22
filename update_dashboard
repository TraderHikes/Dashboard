"""
TraderHikes Dashboard — Daily Data Pipeline
============================================
Runs automatically via GitHub Actions at 4:30 PM IST every trading day.

What it does:
1. Fetches CMP for all open positions → updates open_trades.cmp
2. Fetches Nifty 500 level → saves to portfolio snapshot
3. Calculates portfolio value for today → saves snapshot
4. Fetches market breadth (% stocks above 21/50/200 EMA)
5. Updates market_breadth table

Data source: yfinance (NSE data, ~15 min delay, free)
"""

import os
import sys
import json
from datetime import datetime, date
import pytz

# ── Install dependencies if missing ──────────────────────────
try:
    import yfinance as yf
except ImportError:
    os.system("pip install yfinance --quiet")
    import yfinance as yf

try:
    from supabase import create_client, Client
except ImportError:
    os.system("pip install supabase --quiet")
    from supabase import create_client, Client

import pandas as pd

# ════════════════════════════════════════════════════════════
# CONFIG — values come from GitHub Secrets (never hardcoded)
# ════════════════════════════════════════════════════════════
SUPABASE_URL         = os.environ.get("SUPABASE_URL", "https://kqndpeflwuztzhixibih.supabase.co")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

if not SUPABASE_SERVICE_KEY:
    print("ERROR: SUPABASE_SERVICE_KEY environment variable not set")
    sys.exit(1)

# IST timezone
IST = pytz.timezone("Asia/Kolkata")
TODAY = datetime.now(IST).date()
TODAY_STR = TODAY.strftime("%Y-%m-%d")

print(f"\n{'='*55}")
print(f"TraderHikes Data Pipeline — {TODAY_STR}")
print(f"{'='*55}\n")

# ── Supabase client ──────────────────────────────────────────
sb: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

# ════════════════════════════════════════════════════════════
# STEP 1 — Fetch CMP for all open trades
# ════════════════════════════════════════════════════════════
def fetch_cmp_for_open_trades():
    print("📈 Step 1: Fetching CMP for open trades...")

    # Get all open trades from Supabase
    result = sb.table("open_trades").select("id, symbol, entry_price, quantity, cmp").execute()
    trades = result.data

    if not trades:
        print("   No open trades found. Skipping.")
        return {}

    print(f"   Found {len(trades)} open trade(s): {[t['symbol'] for t in trades]}")

    cmp_map = {}
    for trade in trades:
        symbol = trade["symbol"].upper().strip()

        # yfinance uses .NS suffix for NSE stocks
        ticker_symbol = f"{symbol}.NS"

        try:
            ticker = yf.Ticker(ticker_symbol)
            # Get last 2 days of data to ensure we have today's price
            hist = ticker.history(period="2d", interval="1d")

            if hist.empty:
                print(f"   ⚠️  No data for {symbol} ({ticker_symbol})")
                continue

            cmp = round(float(hist["Close"].iloc[-1]), 2)
            cmp_map[trade["id"]] = cmp

            pnl = round((cmp - trade["entry_price"]) * trade["quantity"], 2)
            pnl_pct = round((cmp - trade["entry_price"]) / trade["entry_price"] * 100, 2)
            print(f"   ✓ {symbol}: CMP ₹{cmp:,.2f} | P&L: {'+'if pnl>=0 else ''}₹{pnl:,.0f} ({pnl_pct:+.2f}%)")

            # Update Supabase
            sb.table("open_trades").update({
                "cmp": cmp,
                "updated_at": datetime.now(IST).isoformat()
            }).eq("id", trade["id"]).execute()

        except Exception as e:
            print(f"   ⚠️  Error fetching {symbol}: {e}")

    print(f"   Updated {len(cmp_map)} CMP value(s) in Supabase.\n")
    return cmp_map


# ════════════════════════════════════════════════════════════
# STEP 2 — Fetch key index levels
# ════════════════════════════════════════════════════════════
def fetch_index_levels():
    print("📊 Step 2: Fetching index levels...")

    indices = {
        "nifty50":    "^NSEI",
        "sensex":     "^BSESN",
        "nifty500":   "^CRSLDX",    # Nifty 500
        "banknifty":  "^NSEBANK",
        "niftyit":    "^CNXIT",
        "india_vix":  "^INDIAVIX",
    }

    levels = {}
    for name, ticker_sym in indices.items():
        try:
            ticker = yf.Ticker(ticker_sym)
            hist = ticker.history(period="2d", interval="1d")
            if not hist.empty:
                level = round(float(hist["Close"].iloc[-1]), 2)
                levels[name] = level
                print(f"   ✓ {name.upper()}: {level:,.2f}")
        except Exception as e:
            print(f"   ⚠️  Error fetching {name}: {e}")

    print()
    return levels


# ════════════════════════════════════════════════════════════
# STEP 3 — Calculate and save portfolio snapshot
# ════════════════════════════════════════════════════════════
def save_portfolio_snapshot(cmp_map, index_levels):
    print("💼 Step 3: Saving portfolio snapshot...")

    # Get all open trades
    result = sb.table("open_trades").select("*").execute()
    trades = result.data

    # Get previous snapshot for total_capital reference
    prev = sb.table("portfolio_snapshots")\
        .select("*")\
        .order("snapshot_date", desc=True)\
        .limit(1)\
        .execute()

    prev_data = prev.data[0] if prev.data else None
    total_capital = prev_data["total_capital"] if prev_data else 2500000  # ₹25L default

    # Calculate deployed and P&L
    deployed = 0
    unrealised_pnl = 0

    for trade in trades:
        invested = trade["entry_price"] * trade["quantity"]
        deployed += invested
        if trade.get("cmp"):
            unrealised_pnl += (trade["cmp"] - trade["entry_price"]) * trade["quantity"]

    cash_available = total_capital - deployed
    portfolio_value = deployed + unrealised_pnl + cash_available

    # Calculate cumulative return
    # Get first ever snapshot for baseline
    first = sb.table("portfolio_snapshots")\
        .select("portfolio_value, snapshot_date")\
        .order("snapshot_date", desc=False)\
        .limit(1)\
        .execute()

    cumulative_return_pct = 0
    if first.data:
        first_value = first.data[0]["portfolio_value"]
        if first_value and first_value > 0:
            cumulative_return_pct = round((portfolio_value - first_value) / first_value * 100, 2)
    else:
        # First snapshot — return is 0
        cumulative_return_pct = 0

    nifty500_level = index_levels.get("nifty500", None)
    nifty50_level  = index_levels.get("nifty50", None)

    snapshot = {
        "snapshot_date":         TODAY_STR,
        "total_capital":         round(total_capital, 2),
        "deployed":              round(deployed, 2),
        "cash_available":        round(cash_available, 2),
        "portfolio_value":       round(portfolio_value, 2),
        "nifty500_level":        nifty500_level,
        "cumulative_return_pct": cumulative_return_pct,
    }

    # Upsert — update if today's row exists, insert if not
    sb.table("portfolio_snapshots").upsert(
        snapshot,
        on_conflict="snapshot_date"
    ).execute()

    print(f"   ✓ Total Capital:    ₹{total_capital:,.0f}")
    print(f"   ✓ Deployed:         ₹{deployed:,.0f}")
    print(f"   ✓ Unrealised P&L:   {'+'if unrealised_pnl>=0 else ''}₹{unrealised_pnl:,.0f}")
    print(f"   ✓ Cash Available:   ₹{cash_available:,.0f}")
    print(f"   ✓ Portfolio Value:  ₹{portfolio_value:,.0f}")
    print(f"   ✓ Cumulative Return: {cumulative_return_pct:+.2f}%")
    print(f"   ✓ Nifty 500:        {nifty500_level}")
    print(f"   Snapshot saved for {TODAY_STR}.\n")


# ════════════════════════════════════════════════════════════
# STEP 4 — Calculate market breadth
# ════════════════════════════════════════════════════════════
def calculate_market_breadth(index_levels):
    print("📡 Step 4: Calculating market breadth...")

    # Nifty 500 constituent symbols
    # We use a representative sample of ~100 liquid stocks
    # for breadth calculation (full 500 would take too long)
    nifty500_sample = [
        "RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK","HINDUNILVR","ITC","SBIN",
        "BAJFINANCE","BHARTIARTL","KOTAKBANK","LT","AXISBANK","ASIANPAINT","MARUTI",
        "TITAN","SUNPHARMA","NESTLEIND","WIPRO","ULTRACEMCO","POWERGRID","NTPC",
        "HCLTECH","TECHM","TATAMOTORS","BAJAJFINSV","DIVISLAB","DRREDDY","CIPLA",
        "ADANIPORTS","HINDALCO","JSWSTEEL","TATASTEEL","ONGC","COALINDIA","BPCL",
        "IOC","GRASIM","BRITANNIA","HEROMOTOCO","EICHERMOT","BAJAJ-AUTO","SHREECEM",
        "UPL","TATACONSUM","INDUSINDBK","DMART","PIDILITIND","SIEMENS","ABB",
        "HAVELLS","POLYCAB","DIXON","PERSISTENT","MPHASIS","COFORGE","LTIM",
        "TRENT","ZOMATO","NYKAA","PAYTM","IRCTC","INDIGO","TATACOMM","MARICO",
        "DABUR","GODREJCP","COLPAL","BERGEPAINT","KANSAINER","CUMMINSIND","VOLTAS",
        "WHIRLPOOL","BLUEDART","CONCOR","APOLLOHOSP","FORTIS","MAXHEALTH","ASTRAL",
        "SUPREMEIND","AARTIIND","ATUL","DEEPAKNTR","PIIND","SRF","ALKYLAMINE",
        "LALPATHLAB","METROPOLIS","THYROCARE","AJANTPHARM","TORNTPHARM","ALKEM",
        "IPCALAB","NATCOPHARMA","AUROPHARMA","GLENMARK","BIOCON","ABBOTINDIA",
        "PFIZER","SANOFI","GLAXO","JUBLPHARMA","GRANULES","SOLARA","LAURUSLABS",
    ]

    print(f"   Fetching data for {len(nifty500_sample)} stocks (representative sample)...")

    above_21  = 0
    above_50  = 0
    above_200 = 0
    advancing = 0
    declining = 0
    new_52w_high = 0
    new_52w_low  = 0
    processed = 0

    for symbol in nifty500_sample:
        try:
            ticker = yf.Ticker(f"{symbol}.NS")
            # Get 1 year of data to calculate all EMAs
            hist = ticker.history(period="1y", interval="1d")

            if hist.empty or len(hist) < 5:
                continue

            close = hist["Close"]

            # Calculate EMAs
            ema21  = close.ewm(span=21,  adjust=False).mean()
            ema50  = close.ewm(span=50,  adjust=False).mean()
            ema200 = close.ewm(span=200, adjust=False).mean()

            latest_close = close.iloc[-1]
            prev_close   = close.iloc[-2] if len(close) >= 2 else latest_close

            # Above EMA checks
            if latest_close > ema21.iloc[-1]:  above_21  += 1
            if latest_close > ema50.iloc[-1]:  above_50  += 1
            if latest_close > ema200.iloc[-1]: above_200 += 1

            # Advance / Decline
            if latest_close > prev_close:  advancing += 1
            elif latest_close < prev_close: declining += 1

            # 52-week high / low
            high_52w = close.max()
            low_52w  = close.min()
            if latest_close >= high_52w * 0.995: new_52w_high += 1
            if latest_close <= low_52w  * 1.005: new_52w_low  += 1

            processed += 1

        except Exception:
            continue

    if processed == 0:
        print("   ⚠️  Could not process any stocks. Skipping breadth update.")
        return

    # Calculate percentages
    pct_21  = round(above_21  / processed * 100, 1)
    pct_50  = round(above_50  / processed * 100, 1)
    pct_200 = round(above_200 / processed * 100, 1)

    print(f"   ✓ Processed: {processed} stocks")
    print(f"   ✓ Above 21  EMA: {pct_21}%  ({above_21}/{processed})")
    print(f"   ✓ Above 50  EMA: {pct_50}%  ({above_50}/{processed})")
    print(f"   ✓ Above 200 EMA: {pct_200}% ({above_200}/{processed})")
    print(f"   ✓ Advancing: {advancing} | Declining: {declining}")
    print(f"   ✓ 52W Highs: {new_52w_high} | 52W Lows: {new_52w_low}")

    breadth_data = {
        "snapshot_date":       TODAY_STR,
        "pct_above_21ema":     pct_21,
        "pct_above_50ema":     pct_50,
        "pct_above_200ema":    pct_200,
        "total_stocks":        processed,
        "above_21ema_count":   above_21,
        "above_50ema_count":   above_50,
        "above_200ema_count":  above_200,
        "advancing":           advancing,
        "declining":           declining,
        "new_52w_highs":       new_52w_high,
        "new_52w_lows":        new_52w_low,
        "nifty50_close":       index_levels.get("nifty50"),
        "india_vix":           index_levels.get("india_vix"),
    }

    sb.table("market_breadth").upsert(
        breadth_data,
        on_conflict="snapshot_date"
    ).execute()

    print(f"   Market breadth saved for {TODAY_STR}.\n")


# ════════════════════════════════════════════════════════════
# STEP 5 — Summary report
# ════════════════════════════════════════════════════════════
def print_summary():
    print("="*55)
    print("✅ Pipeline complete!")
    print(f"   Date: {TODAY_STR}")
    print(f"   Time: {datetime.now(IST).strftime('%I:%M %p IST')}")
    print("   Dashboard will reflect updated data on next load.")
    print("="*55 + "\n")


# ════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════
if __name__ == "__main__":
    try:
        cmp_map      = fetch_cmp_for_open_trades()
        index_levels = fetch_index_levels()
        save_portfolio_snapshot(cmp_map, index_levels)
        calculate_market_breadth(index_levels)
        print_summary()

    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
