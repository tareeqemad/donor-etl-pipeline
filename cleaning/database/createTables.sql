-- Create enum type for contributor type
CREATE TYPE public."Enum_Type" AS ENUM (
    'individual',
    'organization'
);

-- Contributors table
CREATE TABLE public.contributors (
    id SERIAL PRIMARY KEY,
    full_name VARCHAR(255) NOT NULL,
    contributor_type public."Enum_Type" NOT NULL
);

-- Contributor addresses
CREATE TABLE public.contributor_addresses (
    id SERIAL PRIMARY KEY,
    contributor_id INT NOT NULL REFERENCES public.contributors(id) ON DELETE CASCADE,
    street_1 VARCHAR(255),
    street_2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(20),
    zip VARCHAR(20),
    lat DOUBLE PRECISION,
    lng DOUBLE PRECISION
);

-- Contributor business
CREATE TABLE public.contributor_business (
    id SERIAL PRIMARY KEY,
    contributor_id INT NOT NULL REFERENCES public.contributors(id) ON DELETE CASCADE,
    business_address VARCHAR(255),
    employer VARCHAR(255),
    occupation VARCHAR(255)
);

-- Contributions
CREATE TABLE public.contributions (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50) NOT NULL,
    contributor_id INT NOT NULL REFERENCES public.contributors(id) ON DELETE CASCADE,
    contribution_receipt_amount NUMERIC(14,2),
    contribution_receipt_date DATE,
    sub_id VARCHAR(50),
    year_cycle INT,
    contributor_year INT,
    committee_id VARCHAR(50)
);
