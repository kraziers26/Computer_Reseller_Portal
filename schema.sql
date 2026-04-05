-- =============================================================================
-- iGamer Corp — Invoice Management System
-- PostgreSQL Schema
-- =============================================================================

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "pgcrypto";


-- =============================================================================
-- dim_companies
-- Master list of business entities. Referenced by all other tables.
-- =============================================================================
CREATE TABLE dim_companies (
    company_id          SERIAL PRIMARY KEY,
    company_name        TEXT NOT NULL UNIQUE,
    company_short_code  TEXT NOT NULL UNIQUE,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    last_modified_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Seed data
INSERT INTO dim_companies (company_name, company_short_code) VALUES
    ('Sunny Enterprise', 'SE'),
    ('Medara Studio',    'MS'),
    ('Santech',          'ST');


-- =============================================================================
-- dim_users
-- All users who submit invoices or have admin access.
-- =============================================================================
CREATE TABLE dim_users (
    user_id             INTEGER PRIMARY KEY,       -- Preserve existing 101-115
    username            TEXT NOT NULL,
    full_name           TEXT,
    email               TEXT UNIQUE,
    phone               TEXT,
    telegram_id         TEXT UNIQUE,               -- Telegram user ID for bot
    managed_by          TEXT NOT NULL DEFAULT 'Admin',  -- 'Admin' or 'Self'
    is_admin            BOOLEAN NOT NULL DEFAULT FALSE,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    -- Portal access
    portal_password_hash TEXT,                          -- bcrypt hashed, never plain text
    portal_role         TEXT NOT NULL DEFAULT 'none'
                            CHECK (portal_role IN ('admin', 'submitter', 'none')),
    last_login_at       TIMESTAMP,
    failed_login_count  INTEGER NOT NULL DEFAULT 0,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    last_modified_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE SEQUENCE dim_users_user_id_seq START 116 INCREMENT 1;
ALTER TABLE dim_users ALTER COLUMN user_id SET DEFAULT nextval('dim_users_user_id_seq');

-- Seed existing 15 users (email/phone/telegram_id to be filled via portal)
INSERT INTO dim_users (user_id, username, managed_by, is_admin, portal_role) VALUES
    (101, 'Ronald S',      'Admin', TRUE,  'admin'),
    (102, 'Gaby V',        'Admin', TRUE,  'admin'),
    (103, 'David S',       'Self',  FALSE, 'none'),
    (104, 'Laura R',       'Self',  FALSE, 'none'),
    (105, 'Olga C',        'Admin', FALSE, 'none'),
    (106, 'Suhail M',      'Admin', FALSE, 'none'),
    (107, 'Javier F',      'Self',  FALSE, 'none'),
    (108, 'Judy A',        'Self',  FALSE, 'none'),
    (109, 'Blanca M',      'Admin', FALSE, 'none'),
    (110, 'Max C',         'Admin', FALSE, 'none'),
    (111, 'Ulises M',      'Admin', FALSE, 'none'),
    (112, 'Alexis M',      'Admin', FALSE, 'none'),
    (113, 'Apollo C',      'Admin', FALSE, 'none'),
    (114, 'Isabella V',    'Admin', FALSE, 'none'),
    (115, 'Max Sanchez',   'Admin', FALSE, 'none');


-- =============================================================================
-- user_companies
-- Bridge table — one user can belong to multiple companies.
-- =============================================================================
CREATE TABLE user_companies (
    user_id     INTEGER NOT NULL REFERENCES dim_users(user_id) ON DELETE CASCADE,
    company_id  INTEGER NOT NULL REFERENCES dim_companies(company_id) ON DELETE CASCADE,
    PRIMARY KEY (user_id, company_id)
);

-- Seed based on existing card/transaction data
-- Sunny Enterprise members
INSERT INTO user_companies (user_id, company_id)
SELECT u.user_id, c.company_id
FROM dim_users u, dim_companies c
WHERE c.company_short_code = 'SE'
  AND u.username IN (
    'Ronald S', 'Gaby V', 'David S', 'Laura R',
    'Blanca M', 'Suhail M', 'Max Sanchez'
  );

-- Medara Studio members
INSERT INTO user_companies (user_id, company_id)
SELECT u.user_id, c.company_id
FROM dim_users u, dim_companies c
WHERE c.company_short_code = 'MS'
  AND u.username IN (
    'Gaby V', 'Apollo C', 'Alexis M', 'Judy A',
    'Olga C', 'Javier F', 'Isabella V'
  );


-- =============================================================================
-- dim_cards
-- All business credit cards.
-- card_id stored as TEXT to preserve leading zeros (e.g. 0529).
-- cashback_rate stored as decimal (1.5% = 0.015).
-- =============================================================================
CREATE TABLE dim_cards (
    card_id             TEXT PRIMARY KEY,          -- Last 4 digits, text, zero-padded
    card_name           TEXT NOT NULL,
    card_brand          TEXT NOT NULL,
    user_id             INTEGER REFERENCES dim_users(user_id) ON DELETE SET NULL,
    company_id          INTEGER NOT NULL REFERENCES dim_companies(company_id),
    credit_limit        NUMERIC(12, 2),
    cashback_rate       NUMERIC(6, 4) NOT NULL,    -- e.g. 0.0150 for 1.5%
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    last_modified_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Seed from DIM_Cards.xlsx (deduplicated, zero-padded, decimal rates)
-- Sunny Enterprise cards
INSERT INTO dim_cards (card_id, card_name, card_brand, user_id, company_id, credit_limit, cashback_rate) VALUES
    ('0529', 'Ink Business Cash',   'Chase',            101, 1, 33000,  0.0100),
    ('9747', 'Ink Unlimited',       'Chase',            101, 1, 100000, 0.0150),
    ('2265', 'Sapphire',            'Chase',            101, 1, 15700,  0.0100),
    ('7610', 'Chase Prime Visa',    'Chase',            NULL,1, 15700,  0.0100),  -- Everyone
    ('8666', 'Walmart Rewards',     'Capital One',      101, 1, 3000,   0.0100),
    ('1003', 'Amex Amazon',         'American Express', 101, 1, 36000,  0.0100),
    ('4908', 'Ink Unlimited',       'Chase',            109, 1, 100000, 0.0150),
    ('3015', 'Ink Unlimited',       'Chase',            103, 1, 100000, 0.0150),
    ('4360', 'Ink Unlimited',       'Chase',            NULL,1, 100000, 0.0150),  -- Esteban Toral
    ('4644', 'Ink Unlimited',       'Chase',            102, 1, 100000, 0.0150),
    ('1883', 'Ink Unlimited',       'Chase',            104, 1, 100000, 0.0150),
    ('7719', 'Ink Unlimited',       'Chase',            106, 1, 100000, 0.0150),
    ('1029', 'Amex Amazon',         'American Express', 103, 1, 36000,  0.0100),
    ('4189', 'Ink Business Cash',   'Chase',            106, 1, 33000,  0.0100),
    ('7423', 'Ink Business Cash',   'Chase',            109, 1, 33000,  0.0100),
    ('4811', 'Ink Unlimited',       'Chase',            115, 1, 100000, 0.0150),
    ('1299', 'Apple Card',          'Apple',            101, 1, 6750,   0.0300);

-- Medara Studio cards
INSERT INTO dim_cards (card_id, card_name, card_brand, user_id, company_id, credit_limit, cashback_rate, is_active) VALUES
    ('3364', 'Ink Unlimited',       'Chase',            102, 2, 100000, 0.0150, TRUE),
    ('9004', 'Ink Unlimited',       'Chase',            113, 2, 100000, 0.0150, TRUE),
    ('2710', 'Ink Unlimited',       'Chase',            112, 2, 100000, 0.0150, TRUE),   -- active for Alexis M
    ('1356', 'Ink Unlimited',       'Chase',            112, 2, 100000, 0.0150, FALSE),  -- inactive for Alexis M
    ('1231', 'Ink Unlimited',       'Chase',            108, 2, 100000, 0.0150, TRUE),
    ('1038', 'Ink Unlimited',       'Chase',            105, 2, 100000, 0.0150, TRUE),
    ('8299', 'Ink Unlimited',       'Chase',            NULL,2, 100000, 0.0150, TRUE),   -- shared/Xavier
    ('1070', 'Business Premier',    'Chase',            102, 2, 15000,  0.0250, TRUE),
    ('5333', 'Business Premier',    'Chase',            113, 2, 15000,  0.0250, TRUE),
    ('1448', 'Business Premier',    'Chase',            112, 2, 15000,  0.0250, TRUE),
    ('4498', 'Business Premier',    'Chase',            108, 2, 15000,  0.0250, TRUE),
    ('7633', 'Business Premier',    'Chase',            105, 2, 15000,  0.0250, TRUE),
    ('1478', 'Business Premier',    'Chase',            107, 2, 15000,  0.0250, TRUE),
    ('2678', 'Business Premier',    'Chase',            112, 2, 15000,  0.0250, TRUE),
    ('2633', 'Wells Fargo 2%',      'Wells Fargo',      102, 2, 25000,  0.0200, TRUE),
    ('1745', 'Business Premier',    'Chase',            114, 2, 15000,  0.0250, TRUE),
    ('3536', 'Chase Prime Visa',    'Chase',            NULL,2, 6000,   0.0300, TRUE),   -- Everyone
    ('4253', 'PayPal',              'PayPal',           114, 2, 7000,   0.0150, TRUE),
    ('6025', 'Ink Unlimited',       'Chase',            114, 2, 100000, 0.0150, TRUE);


-- =============================================================================
-- transactions
-- One row per order. Core reporting table.
-- =============================================================================
CREATE TABLE transactions (
    transaction_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_number            TEXT NOT NULL,
    retailer                TEXT NOT NULL CHECK (retailer IN (
                                'Amazon','Best Buy','Costco','Walmart','Apple')),
    purchase_date           DATE NOT NULL,
    purchase_year_month     TEXT NOT NULL,          -- e.g. '2026-03'
    user_id                 INTEGER REFERENCES dim_users(user_id) ON DELETE SET NULL,
    company_id              INTEGER REFERENCES dim_companies(company_id) ON DELETE SET NULL,
    card_id                 TEXT REFERENCES dim_cards(card_id) ON DELETE SET NULL,
    price_total             NUMERIC(12, 2) NOT NULL,
    costco_taxes_paid       NUMERIC(12, 2),         -- Costco only, NULL otherwise
    cashback_rate           NUMERIC(6, 4),          -- Copied from dim_cards at write time
    cashback_value          NUMERIC(12, 2),         -- price_total * cashback_rate
    commission_type         TEXT NOT NULL DEFAULT 'standard'
                                CHECK (commission_type IN ('standard', 'special')),
    commission_fixed_per_unit NUMERIC(10, 2),       -- Only for commission_type = 'special'
    commission_amount       NUMERIC(12, 2),         -- Computed at write time
    fulfillment_method      TEXT NOT NULL DEFAULT 'Delivery'
                                CHECK (fulfillment_method IN ('Delivery', 'Store Pick Up')),
    invoice_file_path       TEXT,
    review_status           TEXT NOT NULL DEFAULT 'Pending'
                                CHECK (review_status IN (
                                    'Pending', 'Auto-approved', 'Reviewed', 'Flagged')),
    review_date             DATE,
    print_date              DATE,
    print_batch_id          TEXT,
    is_duplicate            BOOLEAN NOT NULL DEFAULT FALSE,
    submitted_by_email      TEXT,                   -- Fallback if user_id unresolved
    submitted_at            TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Indexes for common query patterns
CREATE INDEX idx_transactions_user        ON transactions(user_id);
CREATE INDEX idx_transactions_company     ON transactions(company_id);
CREATE INDEX idx_transactions_card        ON transactions(card_id);
CREATE INDEX idx_transactions_retailer    ON transactions(retailer);
CREATE INDEX idx_transactions_year_month  ON transactions(purchase_year_month);
CREATE INDEX idx_transactions_review      ON transactions(review_status);
CREATE INDEX idx_transactions_duplicate   ON transactions(is_duplicate) WHERE is_duplicate = TRUE;
CREATE INDEX idx_transactions_print_batch ON transactions(print_batch_id) WHERE print_batch_id IS NOT NULL;

-- Unique constraint for duplicate detection
CREATE UNIQUE INDEX idx_transactions_dedup
    ON transactions(order_number, retailer)
    WHERE is_duplicate = FALSE;


-- =============================================================================
-- transaction_items
-- One row per line item within an order. Drill-down only.
-- =============================================================================
CREATE TABLE transaction_items (
    item_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id      UUID NOT NULL REFERENCES transactions(transaction_id)
                            ON DELETE CASCADE,
    item_description    TEXT NOT NULL,
    sku_model_color     TEXT,
    quantity            INTEGER NOT NULL CHECK (quantity > 0),
    unit_price          NUMERIC(12, 2) NOT NULL,
    line_total          NUMERIC(12, 2) NOT NULL
);

CREATE INDEX idx_items_transaction ON transaction_items(transaction_id);


-- =============================================================================
-- Trigger: auto-update last_modified_at on dim_companies, dim_users, dim_cards
-- =============================================================================
CREATE OR REPLACE FUNCTION set_last_modified()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_modified_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_companies_modified
    BEFORE UPDATE ON dim_companies
    FOR EACH ROW EXECUTE FUNCTION set_last_modified();

CREATE TRIGGER trg_users_modified
    BEFORE UPDATE ON dim_users
    FOR EACH ROW EXECUTE FUNCTION set_last_modified();

CREATE TRIGGER trg_cards_modified
    BEFORE UPDATE ON dim_cards
    FOR EACH ROW EXECUTE FUNCTION set_last_modified();


-- =============================================================================
-- View: commission summary (used by payroll report)
-- =============================================================================
CREATE VIEW v_commission_summary AS
SELECT
    t.purchase_year_month,
    u.user_id,
    u.username,
    c.company_name,
    COUNT(t.transaction_id)                         AS order_count,
    SUM(t.price_total)                              AS total_purchases,
    SUM(t.commission_amount)                        AS total_commission,
    SUM(t.cashback_value)                           AS total_cashback
FROM transactions t
LEFT JOIN dim_users u      ON t.user_id    = u.user_id
LEFT JOIN dim_companies c  ON t.company_id = c.company_id
WHERE t.is_duplicate = FALSE
  AND t.review_status != 'Flagged'
GROUP BY t.purchase_year_month, u.user_id, u.username, c.company_name
ORDER BY t.purchase_year_month DESC, u.username;


-- =============================================================================
-- View: Costco tax reclaim tracker
-- =============================================================================
CREATE VIEW v_costco_tax_reclaim AS
SELECT
    purchase_year_month,
    company_id,
    COUNT(*)                        AS order_count,
    SUM(price_total)                AS total_purchases,
    SUM(costco_taxes_paid)          AS total_taxes_to_reclaim
FROM transactions
WHERE retailer = 'Costco'
  AND costco_taxes_paid > 0
  AND is_duplicate = FALSE
GROUP BY purchase_year_month, company_id
ORDER BY purchase_year_month DESC;


-- =============================================================================
-- View: pending review queue (admin dashboard)
-- =============================================================================
CREATE VIEW v_pending_review AS
SELECT
    t.transaction_id,
    t.submitted_at,
    t.retailer,
    t.order_number,
    t.purchase_date,
    u.username          AS submitted_by,
    c.company_name,
    t.price_total,
    t.card_id,
    t.review_status,
    t.is_duplicate
FROM transactions t
LEFT JOIN dim_users u     ON t.user_id    = u.user_id
LEFT JOIN dim_companies c ON t.company_id = c.company_id
WHERE t.review_status IN ('Pending', 'Flagged')
   OR t.is_duplicate = TRUE
ORDER BY t.submitted_at DESC;
