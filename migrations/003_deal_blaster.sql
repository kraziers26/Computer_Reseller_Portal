-- ============================================================
-- Migration 003: Deal Blaster
-- Run once against Railway Postgres
-- Command: psql $DATABASE_URL -f 003_deal_blaster.sql
-- ============================================================

-- ── 1. bb_deals ──────────────────────────────────────────────
-- Stores every deal fetched from Best Buy.
-- Upserts on sku so the same product never duplicates.
-- Deals older than 48h are soft-expired via is_active flag.

CREATE TABLE IF NOT EXISTS bb_deals (
    id              SERIAL PRIMARY KEY,
    sku             VARCHAR(50)     NOT NULL UNIQUE,   -- Best Buy product SKU
    name            VARCHAR(255)    NOT NULL,
    brand           VARCHAR(100),
    category        VARCHAR(100),                      -- e.g. "macbook", "gaming", "laptop"
    sale_price      NUMERIC(10,2)   NOT NULL,
    regular_price   NUMERIC(10,2)   NOT NULL,
    discount_pct    SMALLINT        NOT NULL,          -- pre-computed: round((regular-sale)/regular*100)
    score           SMALLINT        NOT NULL,          -- 0–13 Fresh Deal Score
    cpu             VARCHAR(150),                      -- e.g. "Apple M3", "Intel i7-13700H"
    memory          VARCHAR(50),                       -- e.g. "16GB", "32GB"
    url             TEXT,                              -- direct Best Buy product URL
    fetched_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    expires_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW() + INTERVAL '48 hours',
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE
);

-- Index for fast filtered queries on the Deal Blaster page
CREATE INDEX IF NOT EXISTS idx_bb_deals_active     ON bb_deals (is_active);
CREATE INDEX IF NOT EXISTS idx_bb_deals_category   ON bb_deals (category);
CREATE INDEX IF NOT EXISTS idx_bb_deals_brand      ON bb_deals (brand);
CREATE INDEX IF NOT EXISTS idx_bb_deals_score      ON bb_deals (score DESC);
CREATE INDEX IF NOT EXISTS idx_bb_deals_fetched_at ON bb_deals (fetched_at DESC);


-- ── 2. scan_schedules ─────────────────────────────────────────
-- Saved scanner configurations.
-- filters column stores brand/category/price/spec filters as JSON.

CREATE TABLE IF NOT EXISTS scan_schedules (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(100)    NOT NULL,          -- e.g. "Morning Scan"
    trigger_type    VARCHAR(20)     NOT NULL           -- "cron" | "score_alert"
                    CHECK (trigger_type IN ('cron', 'score_alert')),
    cron_expression VARCHAR(50),                       -- e.g. "0 8 * * *" (cron only)
    interval_hours  SMALLINT,                          -- e.g. 2 (score_alert only)
    alert_threshold SMALLINT,                          -- notify when score >= this
    filters         JSONB           NOT NULL DEFAULT '{}',
    -- filters shape:
    -- {
    --   "brands":     ["Apple", "Dell"],
    --   "categories": ["macbook", "gaming"],
    --   "price_min":  0,
    --   "price_max":  2000,
    --   "cpu":        ["Apple M", "Intel"],
    --   "ram":        ["16GB", "32GB"],
    --   "min_score":  9
    -- }
    mode            VARCHAR(20)     NOT NULL DEFAULT 'collect'
                    CHECK (mode IN ('collect', 'notify', 'both')),
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE,
    created_by      INTEGER,                           -- user id (future: FK to users)
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    last_run_at     TIMESTAMPTZ
);


-- ── 3. scan_runs ─────────────────────────────────────────────
-- Audit log of every scan — scheduled or manual.
-- Lets you show "last run" status on the Deal Blaster page.

CREATE TABLE IF NOT EXISTS scan_runs (
    id              SERIAL PRIMARY KEY,
    schedule_id     INTEGER REFERENCES scan_schedules(id) ON DELETE SET NULL,
    -- NULL schedule_id = manual / on-demand run
    triggered_by    VARCHAR(20)     NOT NULL
                    CHECK (triggered_by IN ('manual', 'scheduled')),
    filters_used    JSONB           NOT NULL DEFAULT '{}', -- snapshot of filters at run time
    deals_found     INTEGER         NOT NULL DEFAULT 0,
    new_deals       INTEGER         NOT NULL DEFAULT 0,    -- deals not seen before (new sku)
    run_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    duration_ms     INTEGER,                               -- how long the fetch took
    status          VARCHAR(10)     NOT NULL DEFAULT 'ok'
                    CHECK (status IN ('ok', 'error')),
    error_message   TEXT                                   -- populated on status = 'error'
);

CREATE INDEX IF NOT EXISTS idx_scan_runs_run_at      ON scan_runs (run_at DESC);
CREATE INDEX IF NOT EXISTS idx_scan_runs_schedule_id ON scan_runs (schedule_id);


-- ── Seed: default schedules ───────────────────────────────────
-- These match the two schedules shown in the mockup.
-- You can delete or edit these from the UI once it's live.

INSERT INTO scan_schedules (name, trigger_type, cron_expression, filters, mode)
VALUES (
    'Morning Scan',
    'cron',
    '0 8 * * *',
    '{"categories": ["macbook", "gaming"], "min_score": 9}',
    'collect'
)
ON CONFLICT DO NOTHING;

INSERT INTO scan_schedules (name, trigger_type, interval_hours, alert_threshold, filters, mode)
VALUES (
    'High Score Alert',
    'score_alert',
    2,
    11,
    '{}',
    'both'
)
ON CONFLICT DO NOTHING;


-- ── Done ─────────────────────────────────────────────────────
-- Verify with:
--   \dt bb_deals
--   \dt scan_schedules
--   \dt scan_runs
--   SELECT * FROM scan_schedules;
