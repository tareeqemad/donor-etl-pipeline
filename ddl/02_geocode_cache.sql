-- ==========================================
-- 1) جدول التخزين المؤقت للـ geocode
-- ==========================================
CREATE TABLE IF NOT EXISTS public.geocode_cache (
    key         TEXT PRIMARY KEY,        -- مفتاح مُطبع من (street, city, state, zip, country)
    lat         DOUBLE PRECISION,        -- خط العرض
    lng         DOUBLE PRECISION,        -- خط الطول
    provider    TEXT,                    -- nominatim | google | ...
    updated_at  TIMESTAMPTZ DEFAULT now() -- آخر تحديث
);

-- ==========================================
-- 2) جدول rate limiting (لـ Nominatim global QPS)
-- ==========================================
CREATE TABLE IF NOT EXISTS public.nominatim_rl (
    id       INT PRIMARY KEY,            -- دايماً 1 (singleton row)
    last_ts  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- إدخال السطر الافتراضي إذا مش موجود
INSERT INTO public.nominatim_rl (id)
VALUES (1)
ON CONFLICT (id) DO NOTHING;