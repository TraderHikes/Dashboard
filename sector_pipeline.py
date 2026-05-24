# ============================================================
# MARKET BASECAMP — SECTORAL BREADTH PIPELINE
# Add this to update_dashboard.py
# ============================================================
# This module adds fetch_sector_breadth() which runs at 4:30 PM
# alongside your existing market breadth computation.
# ============================================================

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import date, timedelta
import os
from supabase import create_client

# ── Supabase client (same as your existing setup) ─────────────
SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_SERVICE_KEY']
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── Sector index tickers for RS calculation ────────────────────
SECTOR_INDEX_TICKERS = {
    'BANK':        '^NSEBANK',
    'IT':          '^CNXIT',
    'AUTO':        '^CNXAUTO',
    'PHARMA':      '^CNXPHARMA',
    'FMCG':        '^CNXFMCG',
    'METAL':       '^CNXMETAL',
    'REALTY':      '^CNXREALTY',
    'ENERGY':      '^CNXENERGY',
    'INFRA':       '^CNXINFRA',
    'MEDIA':       '^CNXMEDIA',
    'PSU_BANK':    '^CNXPSUBANK',
    'PVT_BANK':    '^NIFPVTBNK',
    'DEFENCE':     None,          # No direct yfinance ticker; use stock-level calc
    'CHEMICALS':   None,
    'CAP_MARKETS': None,
    'CONS_DUR':    '^CNXCONSUMDURBL',
    'HEALTHCARE':  '^CNXHEALTH',
    'OIL_GAS':     '^CNXOILGAS',
    'TEXTILES':    None,
}

NIFTY500_TICKER = '^CRSLDX'   # Nifty 500 on yfinance


def compute_ema(prices: pd.Series, period: int) -> pd.Series:
    """Exponential Moving Average."""
    return prices.ewm(span=period, adjust=False).mean()


def compute_regime_score(pct_21, pct_50, pct_200, pct_52h, ad_ratio):
    """
    Composite Regime Score (0-100) for a sector.
    
    Weights (same philosophy as market-wide regime score):
      200 EMA breadth   → 30% (long-term structural health)
       50 EMA breadth   → 25% (medium-term trend)
       21 EMA breadth   → 20% (short-term momentum)
      52W High %        → 15% (leadership & strength)
      A/D ratio         → 10% (participation width)
    
    Historical parallel: This mirrors the approach used by
    Lowry Research (founded 1938) — the oldest market breadth
    advisory firm. They've used A/D + new highs as regime
    filters since the 1940s. We've simply extended it with EMAs.
    """
    # Normalise A/D ratio: 0→0 score, 1.0→50, 2.0+→100
    ad_score = min(100, (ad_ratio / 2.0) * 100)
    
    score = (
        pct_200 * 0.30 +
        pct_50  * 0.25 +
        pct_21  * 0.20 +
        pct_52h * 0.15 +
        ad_score * 0.10
    )
    return round(score, 2)


def regime_label(score):
    if score >= 80: return 'STRONG BULL'
    if score >= 60: return 'BULL'
    if score >= 40: return 'NEUTRAL'
    if score >= 20: return 'BEAR'
    return 'STRONG BEAR'


def fetch_sector_breadth():
    """
    Main function: compute breadth metrics for all 19 sectors.
    Runs at 4:30 PM IST after market close.
    
    Strategy:
    1. Load all sector_stocks from Supabase
    2. Group by sector
    3. For each sector: fetch 1Y price history from yfinance
    4. Compute EMA breadth, A/D, 52W stats
    5. Compute RS vs Nifty 500 (1m, 3m, 6m)
    6. Compute Regime Score
    7. Upsert into sector_breadth table
    """
    print("=== SECTORAL BREADTH PIPELINE START ===")
    today = date.today()
    
    # ── Step 1: Load sector stock mappings from Supabase ──────
    resp = supabase.table('sector_stocks').select('*').execute()
    sector_map = {}  # {sector_key: {'name': str, 'category': str, 'symbols': []}}
    
    for row in resp.data:
        sk = row['sector_key']
        if sk not in sector_map:
            sector_map[sk] = {
                'name':     row['sector_name'],
                'category': row['category'],
                'symbols':  []
            }
        sector_map[sk]['symbols'].append(row['symbol'] + '.NS')
    
    print(f"Loaded {len(sector_map)} sectors, "
          f"{sum(len(v['symbols']) for v in sector_map.values())} stock-sector pairs")
    
    # ── Step 2: Fetch Nifty 500 for RS baseline ───────────────
    n500 = yf.download(NIFTY500_TICKER, period='6mo', progress=False)['Close']
    n500_1m  = (n500.iloc[-1] / n500.iloc[-22] - 1) * 100  if len(n500) >= 22  else 0
    n500_3m  = (n500.iloc[-1] / n500.iloc[-66] - 1) * 100  if len(n500) >= 66  else 0
    n500_6m  = (n500.iloc[-1] / n500.iloc[-126]- 1) * 100  if len(n500) >= 126 else 0
    
    # ── Step 3: Process each sector ───────────────────────────
    results = []
    
    for sector_key, meta in sector_map.items():
        symbols   = meta['symbols']
        print(f"\n  Processing {sector_key} ({len(symbols)} stocks)...")
        
        try:
            # Download 1Y history for all stocks in sector
            raw = yf.download(
                symbols,
                period='1y',
                progress=False,
                group_by='ticker',
                auto_adjust=True
            )
            
            above_21, above_50, above_200 = [], [], []
            near_52h, near_52l = [], []
            advances, declines = 0, 0
            valid_stocks = 0
            
            for sym in symbols:
                try:
                    # Handle single vs multi ticker download format
                    if len(symbols) == 1:
                        close = raw['Close']
                    else:
                        close = raw[sym]['Close'] if sym in raw.columns.get_level_values(0) else None
                    
                    if close is None or len(close.dropna()) < 50:
                        continue
                    
                    close = close.dropna()
                    valid_stocks += 1
                    current = close.iloc[-1]
                    prev    = close.iloc[-2] if len(close) > 1 else current
                    
                    # EMA checks
                    ema21  = compute_ema(close, 21).iloc[-1]
                    ema50  = compute_ema(close, 50).iloc[-1]
                    ema200 = compute_ema(close, 200).iloc[-1]
                    
                    above_21.append(1 if current > ema21  else 0)
                    above_50.append(1 if current > ema50  else 0)
                    above_200.append(1 if current > ema200 else 0)
                    
                    # 52-week High/Low (within 3%)
                    high_52w = close.rolling(252).max().iloc[-1]
                    low_52w  = close.rolling(252).min().iloc[-1]
                    near_52h.append(1 if current >= high_52w * 0.97 else 0)
                    near_52l.append(1 if current <= low_52w  * 1.03 else 0)
                    
                    # Advance/Decline
                    if current > prev:   advances += 1
                    elif current < prev: declines  += 1
                
                except Exception as e:
                    continue  # Skip problematic stocks silently
            
            if valid_stocks == 0:
                print(f"    WARNING: No valid data for {sector_key}")
                continue
            
            # ── Compute metrics ─────────────────────────────────
            pct_21  = round(np.mean(above_21)  * 100, 2) if above_21  else 0
            pct_50  = round(np.mean(above_50)  * 100, 2) if above_50  else 0
            pct_200 = round(np.mean(above_200) * 100, 2) if above_200 else 0
            pct_52h = round(np.mean(near_52h)  * 100, 2) if near_52h  else 0
            pct_52l = round(np.mean(near_52l)  * 100, 2) if near_52l  else 0
            ad_ratio = round(advances / max(declines, 1), 2)
            unchanged = valid_stocks - advances - declines
            
            # ── Sector index level + RS vs Nifty 500 ────────────
            idx_ticker = SECTOR_INDEX_TICKERS.get(sector_key)
            idx_level  = None
            idx_chg    = 0
            rs_1m = rs_3m = rs_6m = 0
            
            if idx_ticker:
                try:
                    idx_data = yf.download(idx_ticker, period='6mo', progress=False)['Close']
                    if len(idx_data) >= 2:
                        idx_level = round(float(idx_data.iloc[-1]), 2)
                        idx_chg   = round((idx_data.iloc[-1]/idx_data.iloc[-2] - 1)*100, 2)
                        
                        s_1m  = (idx_data.iloc[-1]/idx_data.iloc[-22]  -1)*100 if len(idx_data)>=22  else 0
                        s_3m  = (idx_data.iloc[-1]/idx_data.iloc[-66]  -1)*100 if len(idx_data)>=66  else 0
                        s_6m  = (idx_data.iloc[-1]/idx_data.iloc[-126] -1)*100 if len(idx_data)>=126 else 0
                        
                        rs_1m = round(float(s_1m - n500_1m), 2)
                        rs_3m = round(float(s_3m - n500_3m), 2)
                        rs_6m = round(float(s_6m - n500_6m), 2)
                except:
                    pass
            
            # ── Regime Score ─────────────────────────────────────
            score = compute_regime_score(pct_21, pct_50, pct_200, pct_52h, ad_ratio)
            label = regime_label(score)
            
            print(f"    {sector_key}: {valid_stocks} stocks | "
                  f"21:{pct_21}% 50:{pct_50}% 200:{pct_200}% | "
                  f"Score:{score} [{label}]")
            
            results.append({
                'date':             str(today),
                'sector_key':       sector_key,
                'sector_name':      meta['name'],
                'category':         meta['category'],
                'total_stocks':     valid_stocks,
                'advances':         advances,
                'declines':         declines,
                'unchanged':        unchanged,
                'pct_above_21ema':  pct_21,
                'pct_above_50ema':  pct_50,
                'pct_above_200ema': pct_200,
                'pct_near_52w_high':pct_52h,
                'pct_near_52w_low': pct_52l,
                'ad_ratio':         ad_ratio,
                'rs_1m':            rs_1m,
                'rs_3m':            rs_3m,
                'rs_6m':            rs_6m,
                'index_level':      idx_level,
                'index_change_pct': idx_chg,
                'regime_score':     score,
                'regime_label':     label,
            })
        
        except Exception as e:
            print(f"    ERROR processing {sector_key}: {e}")
            continue
    
    # ── Step 4: Upsert all results ─────────────────────────────
    if results:
        supabase.table('sector_breadth').upsert(
            results,
            on_conflict='date,sector_key'
        ).execute()
        print(f"\n✅ Saved {len(results)} sector breadth rows for {today}")
    else:
        print("\n⚠️  No results to save")
    
    print("=== SECTORAL BREADTH PIPELINE COMPLETE ===")
    return results


# ── Hook into existing pipeline ────────────────────────────────
# In your update_dashboard.py, inside the 4:30 PM run block, add:
#
#   from sector_pipeline import fetch_sector_breadth
#   fetch_sector_breadth()
#
# OR if you keep everything in one file, paste this function
# and add to your run_mode == 'full' block:
#
#   fetch_sector_breadth()


if __name__ == '__main__':
    fetch_sector_breadth()
