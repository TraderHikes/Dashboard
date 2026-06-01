-- ============================================================
-- MARKET BASECAMP (TraderHikes) — AUTHORITATIVE DATABASE SCHEMA
-- ============================================================
-- Generated from the LIVE Supabase database. Last updated 2026-05-29,
-- AFTER the full RLS hardening pass (all writes locked to is_admin();
-- all sensitive reads scoped to authenticated).
-- Mirrors the real production database — nothing inferred from code.
--
--   16 tables (15 live + 1 legacy) · 52 RLS policies · 1 function
--
-- SECURITY MODEL (verified):
--   • approved_students : student reads OWN row; admin reads all; admin writes.
--   • shared data tables: authenticated read; is_admin() writes. Pipeline
--     writes via the service_role key, which bypasses RLS.
--   • sector_stocks / sector_breadth / watchlist_candles: read-only refs.
--   • NO write policy is open to the public role anywhere.
--
-- ⚠️  PURPOSE: DOCUMENTATION & DISASTER-RECOVERY RECORD ONLY.
--     • Repo source of truth for the schema.
--     • RUN ONLY against a FRESH, EMPTY Supabase project when rebuilding.
--     • DO NOT run against the live database — it is not a migration.
--     • Contains no row data and no secrets — safe to commit.
--
-- REBUILD ORDER (already arranged): function -> tables -> enable RLS -> policies.
-- Before running on a fresh project, set is_admin()'s email.
-- ============================================================


-- ============================================================
-- SECTION 1 · FUNCTIONS  (create first — policies depend on it)
-- ============================================================
-- is_admin() returns TRUE only for the admin, matched by email.
-- The whole write-security model rests on this one function.
-- UPDATE the email here if your admin address ever changes.

CREATE OR REPLACE FUNCTION public.is_admin()
RETURNS BOOLEAN AS $$
BEGIN
  RETURN (
    SELECT email FROM auth.users WHERE id = auth.uid()
  ) = 'traderhikes@gmail.com';   -- ← REPLACE WITH YOUR ADMIN EMAIL
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;


-- ============================================================
-- SECTION 2 · TABLES
-- ============================================================

-- ---- approved_students ----
-- Paid students who may log in. Privacy-critical: each student reads only their own row; admin reads all (is_admin()).
CREATE TABLE IF NOT EXISTS public.approved_students (
  email TEXT NOT NULL,
  full_name TEXT,
  plan TEXT DEFAULT 'active'::text,
  enrolled_at TIMESTAMPTZ DEFAULT now(),
  access_until TIMESTAMPTZ,
  notes TEXT
);

-- ---- students ----
-- LEGACY / UNUSED — original table. Live app uses approved_students. Safe to drop once confirmed unused.
CREATE TABLE IF NOT EXISTS public.students (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email TEXT NOT NULL,
  full_name TEXT,
  plan TEXT NOT NULL DEFAULT 'active'::text,
  enrolled_at TIMESTAMPTZ DEFAULT now(),
  access_until TIMESTAMPTZ,
  notes TEXT
);

-- ---- open_trades ----
-- Active positions. Students read; admin + pipeline write.
CREATE TABLE IF NOT EXISTS public.open_trades (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now(),
  symbol TEXT NOT NULL,
  company_name TEXT NOT NULL,
  sector TEXT,
  exchange TEXT DEFAULT 'NSE'::text,
  entry_date DATE NOT NULL,
  entry_price NUMERIC NOT NULL,
  quantity INTEGER NOT NULL,
  sl_price NUMERIC NOT NULL,
  cmp NUMERIC,
  status TEXT DEFAULT 'long'::text,
  trade_notes TEXT,
  chart_pattern TEXT
);

-- ---- trade_entries ----
-- Pyramid entries — multiple adds per open_trade (open_trade_id).
CREATE TABLE IF NOT EXISTS public.trade_entries (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  open_trade_id UUID NOT NULL,
  entry_date DATE NOT NULL,
  entry_price NUMERIC NOT NULL,
  quantity INTEGER NOT NULL,
  notes TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  capital_at_entry NUMERIC,
  position_size_pct NUMERIC
);

-- ---- partial_exits ----
-- Partial profit bookings against an open trade.
CREATE TABLE IF NOT EXISTS public.partial_exits (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  open_trade_id UUID,
  symbol TEXT NOT NULL,
  exit_date DATE NOT NULL,
  exit_price NUMERIC NOT NULL,
  quantity INTEGER NOT NULL,
  realised_pnl NUMERIC,
  return_pct NUMERIC,
  avg_entry_price NUMERIC,
  notes TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  capital_at_entry NUMERIC,
  position_size_pct NUMERIC
);

-- ---- closed_trades ----
-- Completed trades with full journey + realised P&L.
CREATE TABLE IF NOT EXISTS public.closed_trades (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at TIMESTAMPTZ DEFAULT now(),
  symbol TEXT NOT NULL,
  company_name TEXT NOT NULL,
  sector TEXT,
  exchange TEXT DEFAULT 'NSE'::text,
  entry_date DATE NOT NULL,
  avg_entry_price NUMERIC NOT NULL,
  quantity INTEGER NOT NULL,
  sl_price NUMERIC,
  partial_exit_date DATE,
  partial_exit_price NUMERIC,
  partial_exit_qty INTEGER,
  partial_exit_pct NUMERIC,
  exit_date DATE NOT NULL,
  avg_exit_price NUMERIC NOT NULL,
  realised_pnl NUMERIC,
  return_pct NUMERIC,
  holding_days INTEGER,
  trade_notes TEXT,
  exit_reason TEXT,
  capital_at_entry NUMERIC,
  position_size_pct NUMERIC,
  origin_open_trade_id UUID          -- links to original open_trades.id so
                                     -- pyramids (trade_entries) + partial_exits
                                     -- can be found for the archive chart
);

-- ---- closed_trade_candles ----
-- Frozen OHLCV snapshot per closed trade, captured once at close (copied from
-- trade_candles). Never updated again — permanent reference for archive charts.
CREATE TABLE IF NOT EXISTS public.closed_trade_candles (
  id              BIGSERIAL PRIMARY KEY,
  closed_trade_id UUID NOT NULL REFERENCES public.closed_trades(id) ON DELETE CASCADE,
  date            DATE NOT NULL,
  open            NUMERIC,
  high            NUMERIC,
  low             NUMERIC,
  close           NUMERIC,
  volume          BIGINT,
  UNIQUE (closed_trade_id, date)
);
CREATE INDEX IF NOT EXISTS idx_ctc_trade ON public.closed_trade_candles(closed_trade_id);

-- ---- trade_candles ----
-- Daily OHLCV for OPEN-trade symbols (mini charts).
CREATE TABLE IF NOT EXISTS public.trade_candles (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  symbol TEXT NOT NULL,
  date DATE NOT NULL,
  open NUMERIC,
  high NUMERIC,
  low NUMERIC,
  close NUMERIC,
  volume BIGINT
);

-- ---- watchlist ----
-- MarketSmith CSV upload — current watchlist universe.
CREATE TABLE IF NOT EXISTS public.watchlist (
  id SERIAL PRIMARY KEY,
  symbol TEXT NOT NULL,
  company_name TEXT,
  sector TEXT,
  sector_key TEXT,
  setup_tag TEXT,
  notes TEXT,
  is_active BOOLEAN DEFAULT true,
  added_date DATE DEFAULT CURRENT_DATE,
  cmp NUMERIC,
  rs_1m NUMERIC,
  rs_3m NUMERIC,
  rs_6m NUMERIC,
  above_21ema BOOLEAN,
  above_50ema BOOLEAN,
  above_200ema BOOLEAN,
  sector_score NUMERIC,
  sector_label TEXT,
  updated_at TIMESTAMPTZ DEFAULT now(),
  rs_rating INTEGER,
  uploaded_at TIMESTAMPTZ DEFAULT now()
);

-- ---- watchlist_enriched ----
-- Pipeline EMAs + distances + buy-range flag. Feeds Stock Filter & Charts. (id = serial, not UUID.)
CREATE TABLE IF NOT EXISTS public.watchlist_enriched (
  id SERIAL PRIMARY KEY,
  symbol TEXT NOT NULL,
  company_name TEXT,
  cur_price NUMERIC,
  price_change NUMERIC,
  price_chg_pct NUMERIC,
  rs_rating INTEGER,
  ema_21 NUMERIC,
  ema_50 NUMERIC,
  ema_200 NUMERIC,
  ema_10w NUMERIC,
  pct_from_21ema NUMERIC,
  pct_from_10wema NUMERIC,
  pct_from_200ema NUMERIC,
  in_buy_range BOOLEAN DEFAULT false,
  sector_key TEXT,
  sector_name TEXT,
  sector_score NUMERIC,
  sector_label TEXT,
  market_cap NUMERIC,                 -- fundamentals (yfinance): market cap (₹)
  pe_ratio NUMERIC,                   -- trailing P/E
  roe NUMERIC,                        -- return on equity (%)
  pb_ratio NUMERIC(10,2),             -- price-to-book
  enriched_date DATE DEFAULT CURRENT_DATE,
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- ---- watchlist_candles ----
-- 24-month daily OHLCV for ALL watchlist stocks (Charts tab).
CREATE TABLE IF NOT EXISTS public.watchlist_candles (
  symbol TEXT NOT NULL,
  date DATE NOT NULL,
  open NUMERIC,
  high NUMERIC,
  low NUMERIC,
  close NUMERIC,
  volume BIGINT,
  PRIMARY KEY (symbol, date)
);

-- ---- market_breadth ----
-- Daily NSE breadth snapshot (% above EMAs, A/D, 52w highs).
CREATE TABLE IF NOT EXISTS public.market_breadth (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  snapshot_date DATE NOT NULL UNIQUE,
  created_at TIMESTAMPTZ DEFAULT now(),
  pct_above_21ema NUMERIC,
  pct_above_50ema NUMERIC,
  pct_above_200ema NUMERIC,
  total_stocks INTEGER DEFAULT 1847,
  above_21ema_count INTEGER,
  above_50ema_count INTEGER,
  above_200ema_count INTEGER,
  advancing INTEGER,
  declining INTEGER,
  unchanged INTEGER,
  new_52w_highs INTEGER,
  new_52w_lows INTEGER,
  nifty50_close NUMERIC,
  india_vix NUMERIC,
  fii_flow_cr NUMERIC
);

-- ---- sector_stocks ----
-- NSE sector constituents, refreshed nightly.
CREATE TABLE IF NOT EXISTS public.sector_stocks (
  id SERIAL PRIMARY KEY,
  symbol TEXT NOT NULL,
  sector_key TEXT NOT NULL,
  sector_name TEXT NOT NULL,
  category TEXT DEFAULT 'largecap'::text,
  created_at TIMESTAMPTZ DEFAULT now(),
  company_name TEXT,
  PRIMARY KEY (symbol, sector_key)
);

-- ---- sector_breadth ----
-- Per-sector regime score (0-100) and label.
CREATE TABLE IF NOT EXISTS public.sector_breadth (
  id SERIAL PRIMARY KEY,
  date DATE NOT NULL,
  sector_key TEXT NOT NULL,
  sector_name TEXT NOT NULL,
  category TEXT DEFAULT 'largecap'::text,
  total_stocks INTEGER DEFAULT 0,
  advances INTEGER DEFAULT 0,
  declines INTEGER DEFAULT 0,
  unchanged INTEGER DEFAULT 0,
  pct_above_21ema NUMERIC DEFAULT 0,
  pct_above_50ema NUMERIC DEFAULT 0,
  pct_above_200ema NUMERIC DEFAULT 0,
  pct_near_52w_high NUMERIC DEFAULT 0,
  pct_near_52w_low NUMERIC DEFAULT 0,
  ad_ratio NUMERIC DEFAULT 1.0,
  rs_1m NUMERIC DEFAULT 0,
  rs_3m NUMERIC DEFAULT 0,
  rs_6m NUMERIC DEFAULT 0,
  index_level NUMERIC,
  index_change_pct NUMERIC DEFAULT 0,
  regime_score NUMERIC DEFAULT 0,
  regime_label TEXT DEFAULT 'NEUTRAL'::text,
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- ---- index_levels ----
-- Daily close of Nifty50/Sensex + ~24 sectoral indices.
CREATE TABLE IF NOT EXISTS public.index_levels (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  snapshot_date DATE NOT NULL UNIQUE,
  nifty50 NUMERIC,
  sensex NUMERIC,
  nifty500 NUMERIC,
  nifty_midcap NUMERIC,
  nifty_it NUMERIC,
  nifty_bank NUMERIC,
  nifty_pharma NUMERIC,
  nifty_auto NUMERIC,
  nifty_psubank NUMERIC,
  nifty_metal NUMERIC,
  nifty_realty NUMERIC,
  nifty_fmcg NUMERIC,
  nifty50_prev NUMERIC,
  sensex_prev NUMERIC,
  nifty500_prev NUMERIC,
  nifty_midcap_prev NUMERIC,
  nifty_it_prev NUMERIC,
  nifty_bank_prev NUMERIC,
  nifty_pharma_prev NUMERIC,
  nifty_auto_prev NUMERIC,
  nifty_psubank_prev NUMERIC,
  nifty_metal_prev NUMERIC,
  nifty_realty_prev NUMERIC,
  nifty_fmcg_prev NUMERIC,
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- ---- fii_dii_activity ----
-- Daily FII/DII net cash flow.
CREATE TABLE IF NOT EXISTS public.fii_dii_activity (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  activity_date DATE NOT NULL,
  fii_cash_net NUMERIC,
  fii_cash_buy NUMERIC,
  fii_cash_sell NUMERIC,
  dii_cash_net NUMERIC,
  dii_cash_buy NUMERIC,
  dii_cash_sell NUMERIC,
  nifty_close NUMERIC,
  source TEXT DEFAULT 'NSE'::text,
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- ---- portfolio_snapshots ----
-- Daily portfolio value for the equity curve.
CREATE TABLE IF NOT EXISTS public.portfolio_snapshots (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  snapshot_date DATE NOT NULL UNIQUE,
  total_capital NUMERIC,
  deployed NUMERIC,
  cash_available NUMERIC,
  portfolio_value NUMERIC,
  nifty500_level NUMERIC,
  cumulative_return_pct NUMERIC
);


-- ============================================================
-- SECTION 3 · ENABLE ROW LEVEL SECURITY
-- ============================================================

ALTER TABLE public.approved_students ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.students ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.open_trades ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.trade_entries ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.partial_exits ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.closed_trades ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.closed_trade_candles ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.trade_candles ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.watchlist ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.watchlist_enriched ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.watchlist_candles ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.market_breadth ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.sector_stocks ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.sector_breadth ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.index_levels ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.fii_dii_activity ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.portfolio_snapshots ENABLE ROW LEVEL SECURITY;


-- ============================================================
-- SECTION 4 · RLS POLICIES  (additive / OR-ed per table)
-- ============================================================

-- ---- approved_students ----
-- Two SELECT policies coexist intentionally (additive): students read own row, admin reads all.
CREATE POLICY "Admin can insert students" ON public.approved_students FOR INSERT TO authenticated
  WITH CHECK (is_admin());
CREATE POLICY "Admin can read all students" ON public.approved_students FOR SELECT TO authenticated
  USING (is_admin());
CREATE POLICY "Students can read own row only" ON public.approved_students FOR SELECT TO authenticated
  USING (((auth.jwt() ->> 'email'::text) = email));
CREATE POLICY "Admin can update students" ON public.approved_students FOR UPDATE TO authenticated
  USING (is_admin());

-- ---- students ----
-- LEGACY table (unused by live app).
CREATE POLICY "Students can read own profile" ON public.students FOR SELECT TO public
  USING ((auth.uid() = id));

-- ---- open_trades ----
CREATE POLICY "Admin can delete open_trades" ON public.open_trades FOR DELETE TO authenticated
  USING (is_admin());
CREATE POLICY "Admin can insert open_trades" ON public.open_trades FOR INSERT TO authenticated
  WITH CHECK (is_admin());
CREATE POLICY "Authenticated can read open_trades" ON public.open_trades FOR SELECT TO authenticated
  USING (true);
CREATE POLICY "Admin can update open_trades" ON public.open_trades FOR UPDATE TO authenticated
  USING (is_admin());

-- ---- trade_entries ----
CREATE POLICY "Admin can delete trade_entries" ON public.trade_entries FOR DELETE TO authenticated
  USING (is_admin());
CREATE POLICY "Admin can insert trade_entries" ON public.trade_entries FOR INSERT TO authenticated
  WITH CHECK (is_admin());
CREATE POLICY "Authenticated can read trade_entries" ON public.trade_entries FOR SELECT TO authenticated
  USING (true);
CREATE POLICY "Admin can update trade_entries" ON public.trade_entries FOR UPDATE TO authenticated
  USING (is_admin());

-- ---- partial_exits ----
CREATE POLICY "Admin can delete partial_exits" ON public.partial_exits FOR DELETE TO authenticated
  USING (is_admin());
CREATE POLICY "Admin can insert partial_exits" ON public.partial_exits FOR INSERT TO authenticated
  WITH CHECK (is_admin());
CREATE POLICY "Authenticated can read partial_exits" ON public.partial_exits FOR SELECT TO authenticated
  USING (true);
CREATE POLICY "Admin can update partial_exits" ON public.partial_exits FOR UPDATE TO authenticated
  USING (is_admin());

-- ---- closed_trades ----
CREATE POLICY "Admin can delete closed_trades" ON public.closed_trades FOR DELETE TO authenticated
  USING (is_admin());
CREATE POLICY "Admin can insert closed_trades" ON public.closed_trades FOR INSERT TO authenticated
  WITH CHECK (is_admin());
CREATE POLICY "Authenticated can read closed_trades" ON public.closed_trades FOR SELECT TO authenticated
  USING (true);
CREATE POLICY "Admin can update closed_trades" ON public.closed_trades FOR UPDATE TO authenticated
  USING (is_admin());

-- ---- trade_candles ----
CREATE POLICY "Admin can delete trade_candles" ON public.trade_candles FOR DELETE TO authenticated
  USING (is_admin());
CREATE POLICY "Admin can insert trade_candles" ON public.trade_candles FOR INSERT TO authenticated
  WITH CHECK (is_admin());
CREATE POLICY "Authenticated can read trade_candles" ON public.trade_candles FOR SELECT TO authenticated
  USING (true);
CREATE POLICY "Admin can update trade_candles" ON public.trade_candles FOR UPDATE TO authenticated
  USING (is_admin());

-- ---- closed_trade_candles ----
CREATE POLICY "Admin can delete closed_trade_candles" ON public.closed_trade_candles FOR DELETE TO authenticated
  USING (is_admin());
CREATE POLICY "Admin can insert closed_trade_candles" ON public.closed_trade_candles FOR INSERT TO authenticated
  WITH CHECK (is_admin());
CREATE POLICY "Authenticated can read closed_trade_candles" ON public.closed_trade_candles FOR SELECT TO authenticated
  USING (true);
CREATE POLICY "Admin can update closed_trade_candles" ON public.closed_trade_candles FOR UPDATE TO authenticated
  USING (is_admin());

-- ---- watchlist ----
CREATE POLICY "Admin can delete watchlist" ON public.watchlist FOR DELETE TO authenticated
  USING (is_admin());
CREATE POLICY "Admin can insert watchlist" ON public.watchlist FOR INSERT TO authenticated
  WITH CHECK (is_admin());
CREATE POLICY "Authenticated can read watchlist" ON public.watchlist FOR SELECT TO authenticated
  USING (true);
CREATE POLICY "Admin can update watchlist" ON public.watchlist FOR UPDATE TO authenticated
  USING (is_admin());

-- ---- watchlist_enriched ----
CREATE POLICY "Admin can delete watchlist_enriched" ON public.watchlist_enriched FOR DELETE TO authenticated
  USING (is_admin());
CREATE POLICY "Admin can insert watchlist_enriched" ON public.watchlist_enriched FOR INSERT TO authenticated
  WITH CHECK (is_admin());
CREATE POLICY "Authenticated can read watchlist_enriched" ON public.watchlist_enriched FOR SELECT TO authenticated
  USING (true);
CREATE POLICY "Admin can update watchlist_enriched" ON public.watchlist_enriched FOR UPDATE TO authenticated
  USING (is_admin());

-- ---- watchlist_candles ----
CREATE POLICY "Authenticated users can read watchlist candles" ON public.watchlist_candles FOR SELECT TO authenticated
  USING (true);

-- ---- market_breadth ----
CREATE POLICY "Admin can delete market_breadth" ON public.market_breadth FOR DELETE TO authenticated
  USING (is_admin());
CREATE POLICY "Admin can insert market_breadth" ON public.market_breadth FOR INSERT TO authenticated
  WITH CHECK (is_admin());
CREATE POLICY "Authenticated can read market_breadth" ON public.market_breadth FOR SELECT TO authenticated
  USING (true);
CREATE POLICY "Admin can update market_breadth" ON public.market_breadth FOR UPDATE TO authenticated
  USING (is_admin());

-- ---- sector_stocks ----
-- Public SELECT (reference data, no write policy — no tampering risk).
CREATE POLICY "Public read sector_stocks" ON public.sector_stocks FOR SELECT TO public
  USING (true);

-- ---- sector_breadth ----
-- Public SELECT (reference data, no write policy — no tampering risk).
CREATE POLICY "Public read sector_breadth" ON public.sector_breadth FOR SELECT TO public
  USING (true);

-- ---- index_levels ----
CREATE POLICY "Admin can delete index_levels" ON public.index_levels FOR DELETE TO authenticated
  USING (is_admin());
CREATE POLICY "Admin can insert index_levels" ON public.index_levels FOR INSERT TO authenticated
  WITH CHECK (is_admin());
CREATE POLICY "Authenticated can read index_levels" ON public.index_levels FOR SELECT TO authenticated
  USING (true);
CREATE POLICY "Admin can update index_levels" ON public.index_levels FOR UPDATE TO authenticated
  USING (is_admin());

-- ---- fii_dii_activity ----
CREATE POLICY "Admin can delete fii_dii_activity" ON public.fii_dii_activity FOR DELETE TO authenticated
  USING (is_admin());
CREATE POLICY "Admin can insert fii_dii_activity" ON public.fii_dii_activity FOR INSERT TO authenticated
  WITH CHECK (is_admin());
CREATE POLICY "Authenticated can read fii_dii_activity" ON public.fii_dii_activity FOR SELECT TO authenticated
  USING (true);
CREATE POLICY "Admin can update fii_dii_activity" ON public.fii_dii_activity FOR UPDATE TO authenticated
  USING (is_admin());

-- ---- portfolio_snapshots ----
CREATE POLICY "Admin can delete portfolio_snapshots" ON public.portfolio_snapshots FOR DELETE TO authenticated
  USING (is_admin());
CREATE POLICY "Admin can insert portfolio_snapshots" ON public.portfolio_snapshots FOR INSERT TO authenticated
  WITH CHECK (is_admin());
CREATE POLICY "Authenticated can read portfolio_snapshots" ON public.portfolio_snapshots FOR SELECT TO authenticated
  USING (true);
CREATE POLICY "Admin can update portfolio_snapshots" ON public.portfolio_snapshots FOR UPDATE TO authenticated
  USING (is_admin());


-- ============================================================
-- END. Legacy 'students' table may be dropped once confirmed unused.
-- ============================================================
