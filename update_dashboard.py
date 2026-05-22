"""
TraderHikes Dashboard — Daily Data Pipeline
============================================
Runs automatically via GitHub Actions at 4:30 PM IST every trading day.

What it does:
1. Fetches CMP for all open positions → updates open_trades.cmp
2. Fetches key index levels (Nifty 50, Nifty 500, VIX etc.)
3. Calculates portfolio value → saves daily snapshot
4. Fetches FULL Nifty 500 constituent list from NSE India
5. Calculates market breadth (% above 21/50/200 EMA) on all 500 stocks
6. Saves breadth data to Supabase

Data source: yfinance + NSE India CSV (free, ~15 min delay)
"""

import os
import sys
import io
import time
import requests
import pandas as pd
import pytz
from datetime import datetime

# ── Install dependencies ──────────────────────────────────
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

# ════════════════════════════════════════════════════════════
# CONFIG
# ════════════════════════════════════════════════════════════
SUPABASE_URL         = os.environ.get("SUPABASE_URL", "https://kqndpeflwuztzhixibih.supabase.co")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

if not SUPABASE_SERVICE_KEY:
    print("ERROR: SUPABASE_SERVICE_KEY not set")
    sys.exit(1)

IST       = pytz.timezone("Asia/Kolkata")
TODAY     = datetime.now(IST).date()
TODAY_STR = TODAY.strftime("%Y-%m-%d")

print(f"\n{'='*58}")
print(f"  TraderHikes Data Pipeline — {TODAY_STR}")
print(f"{'='*58}\n")

sb: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


# ════════════════════════════════════════════════════════════
# STEP 0 — Fetch full Nifty 500 constituent list from NSE
# ════════════════════════════════════════════════════════════
def fetch_nifty500_symbols():
    """
    Downloads the official Nifty 500 constituent CSV from NSE India.
    Returns a list of NSE symbols (without .NS suffix).
    Falls back to a curated list of 200 stocks if download fails.
    """
    print("📋 Step 0: Fetching Nifty 500 constituent list from NSE...")

    # NSE India hosts the official constituent list here
    url = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://www.nseindia.com/",
    }

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        df = pd.read_csv(io.StringIO(response.text))

        # The CSV has a column called 'Symbol'
        if "Symbol" in df.columns:
            symbols = df["Symbol"].str.strip().tolist()
            print(f"   ✓ Downloaded {len(symbols)} Nifty 500 constituents from NSE")
            return symbols
        else:
            print(f"   ⚠️  Unexpected CSV format. Columns: {df.columns.tolist()}")
            raise ValueError("Symbol column not found")

    except Exception as e:
        print(f"   ⚠️  NSE download failed ({e}). Using curated fallback list...")
        return get_fallback_symbols()


def get_fallback_symbols():
    """
    Curated list of 200 highly liquid Nifty 500 stocks.
    Used only if NSE website is unreachable.
    Covers all major sectors proportionally.
    """
    return [
        # Large Cap — Nifty 50
        "RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK","HINDUNILVR","ITC","SBIN",
        "BAJFINANCE","BHARTIARTL","KOTAKBANK","LT","AXISBANK","ASIANPAINT","MARUTI",
        "TITAN","SUNPHARMA","NESTLEIND","WIPRO","ULTRACEMCO","POWERGRID","NTPC",
        "HCLTECH","TECHM","TATAMOTORS","BAJAJFINSV","DIVISLAB","DRREDDY","CIPLA",
        "ADANIPORTS","HINDALCO","JSWSTEEL","TATASTEEL","ONGC","COALINDIA","BPCL",
        "IOC","GRASIM","BRITANNIA","HEROMOTOCO","EICHERMOT","BAJAJ-AUTO","SHREECEM",
        "UPL","TATACONSUM","INDUSINDBK","SBILIFE","HDFCLIFE","ICICIGI","APOLLOHOSP",

        # Mid Cap — IT & Tech
        "PERSISTENT","MPHASIS","COFORGE","LTIM","KPITTECH","HAPPSTMNDS",
        "MASTEK","BIRLASOFT","HEXAWARE","RATEGAIN","TATAELXSI","CYIENT",

        # Mid Cap — Capital Goods & Infra
        "ABB","SIEMENS","HAVELLS","POLYCAB","DIXON","KAYNES","AMBER","PGEL",
        "CUMMINSIND","THERMAX","BHEL","IRCON","KEC","KALPATPOWR","GPPL",

        # Mid Cap — Consumer & FMCG
        "TRENT","DMART","VMART","ABFRL","MANYAVAR","VEDL","PAGEIND",
        "MARICO","DABUR","GODREJCP","COLPAL","EMAMILTD","JYOTHYLAB",
        "RADICO","MCDOWELL-N","UNITDSPR",

        # Mid Cap — Pharma & Healthcare
        "ASTRAL","TORNTPHARM","ALKEM","IPCALAB","NATCOPHARMA","AUROPHARMA",
        "GLENMARK","BIOCON","JUBLPHARMA","GRANULES","SOLARA","LAURUSLABS",
        "LALPATHLAB","METROPOLIS","THYROCARE","AJANTPHARM","PFIZER","SANOFI",

        # Mid Cap — Banking & Finance
        "FEDERALBNK","BANDHANBNK","IDFCFIRSTB","RBLBANK","DCBBANK",
        "CANBK","BANKBARODA","PNB","UNIONBANK","INDIANB",
        "CHOLAFIN","MUTHOOTFIN","MANAPPURAM","AAVAS","HOMEFIRST",
        "CAMS","CDSL","BSE","MCLEODRUSSEL",

        # Mid Cap — Auto & Ancillaries
        "MOTHERSON","BOSCHLTD","EXIDEIND","AMARAJABAT","BALKRISIND",
        "APOLLOTYRE","CEATLTD","MRF","TIINDIA","SUPRAJIT",

        # Mid Cap — Chemicals
        "PIDILITIND","AARTIIND","ATUL","DEEPAKNTR","PIIND","SRF",
        "ALKYLAMINE","FLUOROCHEM","NAVINFLUOR","FINEORG","TATACHEM",

        # Mid Cap — Realty & Construction
        "DLF","GODREJPROP","OBEROIRLTY","PRESTIGE","PHOENIXLTD",
        "BRIGADE","SOBHA","MAHLIFE","KOLTEPATIL","SUNTECK",

        # Mid Cap — Metals & Mining
        "NMDC","VEDL","NATIONALUM","MOIL","HINDCOPPER",
        "RATNAMANI","APL","SAILESH","MIDHANI",

        # Small/Mid — Diversified
        "IRCTC","CONCOR","BLUEDART","GESHIP","SCI",
        "ZOMATO","NYKAA","PAYTM","DELHIVERY","CARTRADE",
        "INDIGO","SPICEJET","INTERGLOBE",
        "LICI","GICRE","NIACL","STARHEALTH",
    ]


# ════════════════════════════════════════════════════════════
# STEP 1 — Fetch CMP for open trades
# ════════════════════════════════════════════════════════════
def fetch_cmp_for_open_trades():
    print("📈 Step 1: Fetching CMP for open trades...")

    result = sb.table("open_trades").select("id,symbol,entry_price,quantity,sl_price").execute()
    trades = result.data

    if not trades:
        print("   No open trades found. Skipping.\n")
        return {}

    print(f"   Found {len(trades)} open trade(s): {[t['symbol'] for t in trades]}")

    cmp_map = {}
    for trade in trades:
        symbol = trade["symbol"].upper().strip()
        # Handle special NSE symbols (e.g. M&M → MM, L&T → LT)
        ticker_sym = f"{symbol}.NS"

        try:
            ticker = yf.Ticker(ticker_sym)
            hist   = ticker.history(period="2d", interval="1d")

            if hist.empty:
                print(f"   ⚠️  No data for {symbol}")
                continue

            cmp = round(float(hist["Close"].iloc[-1]), 2)
            cmp_map[trade["id"]] = cmp

            pnl     = round((cmp - trade["entry_price"]) * trade["quantity"], 2)
            pnl_pct = round((cmp - trade["entry_price"]) / trade["entry_price"] * 100, 2)

            print(f"   ✓ {symbol}: CMP ₹{cmp:,.2f} | "
                  f"P&L: {'+'if pnl>=0 else ''}₹{pnl:,.0f} ({pnl_pct:+.2f}%)")

            sb.table("open_trades").update({
                "cmp":        cmp,
                "updated_at": datetime.now(IST).isoformat()
            }).eq("id", trade["id"]).execute()

        except Exception as e:
            print(f"   ⚠️  Error fetching {symbol}: {e}")

    print(f"   Updated {len(cmp_map)} position(s).\n")
    return cmp_map


# ════════════════════════════════════════════════════════════
# STEP 2 — Fetch all index levels + save to Supabase
# ════════════════════════════════════════════════════════════
def fetch_index_levels():
    print("📊 Step 2: Fetching index levels...")

    # Full list of indices to fetch and store
    indices = {
        "nifty50":      "^NSEI",
        "sensex":       "^BSESN",
        "nifty500":     "^CRSLDX",
        "nifty_midcap": "^NSEMDCP50",
        "nifty_it":     "^CNXIT",
        "nifty_bank":   "^NSEBANK",
        "nifty_pharma": "^CNXPHARMA",
        "nifty_auto":   "^CNXAUTO",
        "nifty_psubank":"^CNXPSUBANK",
        "nifty_metal":  "^CNXMETAL",
        "nifty_realty": "^CNXREALTY",
        "nifty_fmcg":   "^CNXFMCG",
        "india_vix":    "^INDIAVIX",
    }

    levels = {}      # current close
    prev   = {}      # previous close (for day change)

    for name, sym in indices.items():
        try:
            hist = yf.Ticker(sym).history(period="5d", interval="1d")
            if hist.empty:
                print(f"   ⚠️  No data for {name}")
                continue

            # Get last two valid closes
            closes = hist["Close"].dropna()
            if len(closes) >= 1:
                levels[name] = round(float(closes.iloc[-1]), 2)
            if len(closes) >= 2:
                prev[name]   = round(float(closes.iloc[-2]), 2)

            chg = levels.get(name,0) - prev.get(name,0)
            pct = chg / prev[name] * 100 if prev.get(name) else 0
            print(f"   ✓ {name.upper():16s}: {levels[name]:>10,.2f}  "
                  f"({'+'if chg>=0 else ''}{chg:,.2f}, {pct:+.2f}%)")

        except Exception as e:
            print(f"   ⚠️  {name}: {e}")

    # Save to index_levels table
    if levels:
        row = {"snapshot_date": TODAY_STR}
        for key, val in levels.items():
            if key != "india_vix":
                row[key] = val
        for key, val in prev.items():
            if key != "india_vix":
                row[f"{key}_prev"] = val
        # india_vix goes into market_breadth table (already done)
        # but store it in levels dict for other steps to use
        levels["india_vix"] = levels.get("india_vix", None)

        try:
            sb.table("index_levels").upsert(
                row, on_conflict="snapshot_date"
            ).execute()
            print(f"   ✓ All index levels saved to Supabase for {TODAY_STR}")
        except Exception as e:
            print(f"   ⚠️  Could not save index levels: {e}")

    print()
    return levels


# ════════════════════════════════════════════════════════════
# STEP 3 — Save portfolio snapshot
# ════════════════════════════════════════════════════════════
def save_portfolio_snapshot(cmp_map, index_levels):
    print("💼 Step 3: Saving portfolio snapshot...")

    trades     = sb.table("open_trades").select("*").execute().data
    prev_snaps = sb.table("portfolio_snapshots")\
                   .select("total_capital,snapshot_date")\
                   .order("snapshot_date", desc=True)\
                   .limit(2).execute().data

    # Use most recent total_capital (skip today's if already exists)
    total_capital = 2500000  # ₹25L default
    for s in prev_snaps:
        if s["snapshot_date"] != TODAY_STR and s["total_capital"]:
            total_capital = s["total_capital"]
            break

    deployed       = sum(t["entry_price"] * t["quantity"] for t in trades)
    unrealised_pnl = sum(
        (t["cmp"] - t["entry_price"]) * t["quantity"]
        for t in trades if t.get("cmp")
    )
    cash_available  = total_capital - deployed
    portfolio_value = deployed + unrealised_pnl + cash_available

    # Cumulative return vs first snapshot
    first = sb.table("portfolio_snapshots")\
               .select("portfolio_value,snapshot_date")\
               .order("snapshot_date", desc=False)\
               .limit(1).execute().data

    if first and first[0]["snapshot_date"] != TODAY_STR and first[0]["portfolio_value"]:
        base_value          = first[0]["portfolio_value"]
        cumulative_return   = round((portfolio_value - base_value) / base_value * 100, 2)
    else:
        cumulative_return   = 0.0

    snapshot = {
        "snapshot_date":         TODAY_STR,
        "total_capital":         round(total_capital, 2),
        "deployed":              round(deployed, 2),
        "cash_available":        round(cash_available, 2),
        "portfolio_value":       round(portfolio_value, 2),
        "nifty500_level":        index_levels.get("nifty500"),
        "cumulative_return_pct": cumulative_return,
    }

    sb.table("portfolio_snapshots").upsert(
        snapshot, on_conflict="snapshot_date"
    ).execute()

    print(f"   ✓ Total Capital:     ₹{total_capital:>12,.0f}")
    print(f"   ✓ Deployed:          ₹{deployed:>12,.0f}")
    print(f"   ✓ Unrealised P&L:  {'+'if unrealised_pnl>=0 else ''}₹{unrealised_pnl:>11,.0f}")
    print(f"   ✓ Cash Available:    ₹{cash_available:>12,.0f}")
    print(f"   ✓ Portfolio Value:   ₹{portfolio_value:>12,.0f}")
    print(f"   ✓ Cumulative Return: {cumulative_return:>+.2f}%")
    print(f"   Snapshot saved for {TODAY_STR}.\n")


# ════════════════════════════════════════════════════════════
# STEP 4 — Calculate market breadth on full Nifty 500
# ════════════════════════════════════════════════════════════
def calculate_market_breadth(symbols, index_levels):
    print(f"📡 Step 4: Calculating market breadth on {len(symbols)} Nifty 500 stocks...")
    print(f"   This will take 3–5 minutes. Fetching in batches...\n")

    above_21    = 0
    above_50    = 0
    above_200   = 0
    advancing   = 0
    declining   = 0
    new_52w_high = 0
    new_52w_low  = 0
    processed   = 0
    failed      = 0

    # Process in batches of 20 to avoid rate limits
    BATCH_SIZE = 20

    for batch_start in range(0, len(symbols), BATCH_SIZE):
        batch = symbols[batch_start : batch_start + BATCH_SIZE]
        batch_num = batch_start // BATCH_SIZE + 1
        total_batches = (len(symbols) + BATCH_SIZE - 1) // BATCH_SIZE

        print(f"   Batch {batch_num}/{total_batches}: "
              f"{', '.join(batch[:5])}{'...' if len(batch)>5 else ''}")

        # Fetch all tickers in the batch at once using yfinance download
        ticker_list = " ".join(f"{s}.NS" for s in batch)

        try:
            # Download 1 year of daily data for the entire batch at once
            data = yf.download(
                ticker_list,
                period="1y",
                interval="1d",
                group_by="ticker",
                auto_adjust=True,
                progress=False,
                threads=True,
            )

            for symbol in batch:
                ticker_sym = f"{symbol}.NS"
                try:
                    # Extract close prices for this ticker
                    if len(batch) == 1:
                        close = data["Close"]
                    else:
                        if ticker_sym not in data.columns.get_level_values(0):
                            failed += 1
                            continue
                        close = data[ticker_sym]["Close"]

                    close = close.dropna()
                    if len(close) < 21:
                        failed += 1
                        continue

                    latest = float(close.iloc[-1])
                    prev   = float(close.iloc[-2]) if len(close) >= 2 else latest

                    # EMAs
                    ema21  = float(close.ewm(span=21,  adjust=False).mean().iloc[-1])
                    ema50  = float(close.ewm(span=50,  adjust=False).mean().iloc[-1]) if len(close) >= 50 else None
                    ema200 = float(close.ewm(span=200, adjust=False).mean().iloc[-1]) if len(close) >= 200 else None

                    if latest > ema21:  above_21  += 1
                    if ema50  and latest > ema50:  above_50  += 1
                    if ema200 and latest > ema200: above_200 += 1

                    # Advance / Decline
                    if latest > prev * 1.001:  advancing += 1
                    elif latest < prev * 0.999: declining += 1

                    # 52-week high / low (within 0.5% counts)
                    high_52w = float(close.max())
                    low_52w  = float(close.min())
                    if latest >= high_52w * 0.995: new_52w_high += 1
                    if latest <= low_52w  * 1.005: new_52w_low  += 1

                    processed += 1

                except Exception:
                    failed += 1
                    continue

        except Exception as e:
            print(f"   ⚠️  Batch error: {e}")
            failed += len(batch)
            continue

        # Small pause between batches to be polite to yfinance
        if batch_start + BATCH_SIZE < len(symbols):
            time.sleep(1)

    if processed == 0:
        print("   ⚠️  Could not process any stocks. Skipping breadth update.\n")
        return

    # Calculate percentages
    pct_21  = round(above_21  / processed * 100, 2)
    pct_50  = round(above_50  / processed * 100, 2)
    pct_200 = round(above_200 / processed * 100, 2)

    print(f"\n   ── Nifty 500 Breadth Results ──────────────────")
    print(f"   Processed:        {processed} stocks ({failed} failed/skipped)")
    print(f"   Above 21  EMA:    {pct_21}%  ({above_21} stocks)")
    print(f"   Above 50  EMA:    {pct_50}%  ({above_50} stocks)")
    print(f"   Above 200 EMA:    {pct_200}% ({above_200} stocks)")
    print(f"   Advancing:        {advancing} stocks")
    print(f"   Declining:        {declining} stocks")
    print(f"   A/D Ratio:        {advancing/max(declining,1):.2f}")
    print(f"   New 52W Highs:    {new_52w_high}")
    print(f"   New 52W Lows:     {new_52w_low}")
    print(f"   ──────────────────────────────────────────────")

    breadth = {
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
        breadth, on_conflict="snapshot_date"
    ).execute()

    print(f"   Market breadth saved for {TODAY_STR}.\n")


# ════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════
if __name__ == "__main__":
    try:
        # Fetch Nifty 500 list first
        nifty500_symbols = fetch_nifty500_symbols()
        print()

        # Run all steps
        cmp_map      = fetch_cmp_for_open_trades()
        index_levels = fetch_index_levels()
        save_portfolio_snapshot(cmp_map, index_levels)
        calculate_market_breadth(nifty500_symbols, index_levels)

        print("=" * 58)
        print("✅ Pipeline complete!")
        print(f"   Date: {TODAY_STR}")
        print(f"   Time: {datetime.now(IST).strftime('%I:%M %p IST')}")
        print(f"   Universe: {len(nifty500_symbols)} Nifty 500 stocks")
        print("=" * 58 + "\n")

    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
