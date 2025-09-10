-- =========================================================
-- File: 03_donors_and_donations.sql
-- Description: Schema for donors master table and donations detail table
-- =========================================================

-- ==========================================
-- Master donors table
-- ==========================================
CREATE TABLE IF NOT EXISTS public.donors_master (
    donor_key                    CHAR(64) PRIMARY KEY,
    donor_key_fuzzy              CHAR(64),

    contributor_name             TEXT,
    contributor_first_name       TEXT,
    contributor_last_name        TEXT,
    contributor_street           TEXT,
    contributor_city             TEXT,
    contributor_state            TEXT,
    contributor_zip              TEXT,

    contributor_employer         TEXT,
    contributor_employer_canon   TEXT,
    contributor_occupation       TEXT,
    contributor_occupation_canon TEXT,

    is_individual                BOOLEAN,   -- فرد أو كيان

    lat                          DOUBLE PRECISION,
    lng                          DOUBLE PRECISION,

    total_contributions          NUMERIC(14,2) DEFAULT 0,
    contribution_count           INT DEFAULT 0,
    contribution_years           SMALLINT[],
    first_contribution_date      DATE,
    last_contribution_date       DATE
);

-- Indexes for donors_master
CREATE INDEX IF NOT EXISTS idx_donors_master_fuzzy
    ON public.donors_master(donor_key_fuzzy);

CREATE INDEX IF NOT EXISTS idx_donors_master_individual
    ON public.donors_master(is_individual);

CREATE INDEX IF NOT EXISTS idx_donors_master_location
    ON public.donors_master(contributor_state, contributor_city);


-- ==========================================
-- Donations detail (transactions) table
-- ==========================================
CREATE TABLE IF NOT EXISTS public.donations_detail (
    donation_id                  BIGSERIAL PRIMARY KEY,
    transaction_id               TEXT,
    donor_key                    CHAR(64) REFERENCES public.donors_master(donor_key),

    contribution_receipt_date    DATE NOT NULL,
    contribution_year            SMALLINT,
    contribution_receipt_amount  NUMERIC(14,2) NOT NULL,

    committee_id                 VARCHAR(9),
    two_year_transaction_period  SMALLINT,
    is_refund                    BOOLEAN DEFAULT FALSE,

    created_at                   TIMESTAMP DEFAULT now(),
    updated_at                   TIMESTAMP DEFAULT now(),

    CONSTRAINT donations_detail_txn_uniq UNIQUE (transaction_id, donor_key)
);

-- Indexes for donations_detail
CREATE INDEX IF NOT EXISTS idx_donations_detail_donor
    ON public.donations_detail(donor_key);

CREATE INDEX IF NOT EXISTS idx_donations_detail_date
    ON public.donations_detail(contribution_receipt_date);

CREATE INDEX IF NOT EXISTS idx_donations_detail_committee
    ON public.donations_detail(committee_id);

CREATE INDEX IF NOT EXISTS idx_donations_detail_year
    ON public.donations_detail(contribution_year);

-- =========================================================
-- End of File
-- =========================================================