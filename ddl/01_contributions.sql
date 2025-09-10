-- 1) sync_cursor: لتتبع التقدّم/الاستئناف
CREATE TABLE IF NOT EXISTS public.sync_cursor (
    committee_id      VARCHAR(9) NOT NULL,
    period            SMALLINT NOT NULL,
    watermark_date    DATE NOT NULL,
    last_page_date    DATE,
    last_index        TEXT,
    cursor_updated_at TIMESTAMP DEFAULT now(),
    last_run_at       TIMESTAMP DEFAULT now(),
    CONSTRAINT uq_sync_cursor UNIQUE (committee_id, period)
);

-- 2) contributions: PK داخلي + مفاتيح فريدة وحقول التحليل
CREATE TABLE IF NOT EXISTS public.contributions (
    contrib_row_id               BIGSERIAL PRIMARY KEY,

    sub_id                       BIGINT,                  -- قد يكون NULL
    transaction_id               TEXT,
    two_year_transaction_period  SMALLINT,
    committee_id                 VARCHAR(9),

    contributor_name             TEXT,
    contributor_first_name       TEXT,
    contributor_last_name        TEXT,
    contributor_street_1         TEXT,
    contributor_street_2         TEXT,
    contributor_city             TEXT,
    contributor_state            TEXT,
    contributor_zip              TEXT,
    contributor_employer         TEXT,
    contributor_occupation       TEXT,
    is_individual                BOOLEAN,

    contribution_receipt_date    DATE,
    contributor_year             SMALLINT,                -- EXTRACT(YEAR FROM contribution_receipt_date)
    contribution_receipt_amount  NUMERIC(14,2) DEFAULT 0 NOT NULL,
    contribution_aggregate_ytd   NUMERIC(14,2),
    is_refunded_or_reattributed  BOOLEAN,
    contribution_refunded_amount NUMERIC(14,2),

    line_number                  TEXT,
    line_number_label            TEXT,

    lat                          DOUBLE PRECISION,
    lng                          DOUBLE PRECISION,
    geocode_provider             TEXT,
    geocode_checked_at           TIMESTAMP
);

-- مفاتيح فريدة (CONSTRAINTs لازم عشان FK/سلامة بيانات)
ALTER TABLE public.contributions
    ADD CONSTRAINT uq_contrib_sub_id UNIQUE (sub_id);

ALTER TABLE public.contributions
    ADD CONSTRAINT uq_contrib_txn_fallback
    UNIQUE (transaction_id, contribution_receipt_date, contribution_receipt_amount);

-- فهارس تشغيلية
CREATE INDEX IF NOT EXISTS ix_contrib_comm_date
    ON public.contributions (committee_id, contribution_receipt_date DESC);

CREATE INDEX IF NOT EXISTS ix_contrib_period
    ON public.contributions (two_year_transaction_period);

CREATE INDEX IF NOT EXISTS ix_contrib_is_individual
    ON public.contributions (is_individual);

CREATE INDEX IF NOT EXISTS ix_contrib_comm_indiv_date
    ON public.contributions (committee_id, is_individual, contribution_receipt_date DESC);

CREATE INDEX IF NOT EXISTS ix_contrib_year
    ON public.contributions (contributor_year);

-- 3) contribution_logs: يربط على contrib_row_id (دايماً موجود)
CREATE TABLE IF NOT EXISTS public.contribution_logs (
    id               BIGSERIAL PRIMARY KEY,
    contrib_row_id   BIGINT NOT NULL,
    action_type      VARCHAR(10) NOT NULL,  -- INSERT / UPDATE
    action_time      TIMESTAMP DEFAULT now(),
    notes            TEXT,
    CONSTRAINT fk_contrib_row
      FOREIGN KEY (contrib_row_id)
      REFERENCES public.contributions(contrib_row_id)
      ON DELETE CASCADE
);