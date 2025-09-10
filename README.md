# Donor ETL Pipeline

## Description
A complete ETL pipeline to fetch, geocode, and normalize donor contribution data.  
The project integrates with the FEC API, enriches addresses with latitude/longitude,  
and builds structured donor/donation tables for analysis.

## Components
- **Python Scripts**
  1. `fec_import_public.py` → Fetch raw contributions from the [FEC API](https://api.open.fec.gov/).  
  2. `geocode_worker.py` → Geocode contributor addresses and cache results.  
  3. `etl_clean_donors.py` → Clean/normalize data, generate donor keys, canonicalize occupation/employer, and update master/detail tables.

- **DDL Scripts**
  - `01_contributions.sql`: Raw contributions table.  
  - `02_geocode_cache.sql`: Geocoding cache table.  
  - `03_donors_and_donations.sql`: Master/Detail tables for donors and donations.  

## Requirements
- Python 3.11+
- PostgreSQL
- Python packages:
  - `pandas`
  - `numpy`
  - `sqlalchemy`
  - `psycopg2` (or `psycopg[binary]` for psycopg3)
  - `rapidfuzz`
  - (plus any geocoding library/service you integrate)

## Usage
1. Create the schema using the SQL DDL scripts in order:
   - `01_contributions.sql`
   - `02_geocode_cache.sql`
   - `03_donors_and_donations.sql`
2. Run the ETL pipeline:
   ```bash
   # Step 1: Fetch raw data from FEC
   python fec_import_public.py

   # Step 2: Geocode contributor addresses
   python geocode_worker.py

   # Step 3: Clean & load into master/detail tables
   python etl_clean_donors.py
**3.** Query the structured tables:

donors_master → aggregated donor profiles.

donations_detail → individual contribution transactions.

**Notes**

is_individual = TRUE → occupation/employer canon fields are filled.

is_individual = FALSE → organizational entities, canon fields remain NULL.