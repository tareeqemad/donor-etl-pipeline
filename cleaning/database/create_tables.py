import psycopg2
import logging
from config import load_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("create_tables.log"), logging.StreamHandler()]
)

def create_tables():
    """Create the database tables and sequences for contributions."""
    config = load_config()

    sql_statements = [
        # Enum type
        '''
        CREATE TYPE IF NOT EXISTS public."Enum_Type" AS ENUM (
            'individual',
            'organization'
        );
        ''',

        # Contributors table
        '''
        CREATE TABLE IF NOT EXISTS public.contributors (
            id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            full_name character varying(255),
            contributor_type character varying(50)
        );
        ''',

        # Contributor addresses table
        '''
        CREATE SEQUENCE IF NOT EXISTS public.contributor_addresses_id_seq
        START WITH 1
        INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1;

        CREATE TABLE IF NOT EXISTS public.contributor_addresses (
            id integer DEFAULT nextval('public.contributor_addresses_id_seq') PRIMARY KEY,
            contributor_id integer NOT NULL,
            street_1 character varying(255) NOT NULL,
            street_2 character varying(255),
            city character varying(100) NOT NULL,
            state character varying(2) NOT NULL,
            zip character varying(9) NOT NULL,
            lat numeric,
            lng numeric
        );
        ''',

        # Contributor business table
        '''
        CREATE SEQUENCE IF NOT EXISTS public.contributor_business_id_seq
        START WITH 1
        INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1;

        CREATE TABLE IF NOT EXISTS public.contributor_business (
            id integer DEFAULT nextval('public.contributor_business_id_seq') PRIMARY KEY,
            contributor_id integer NOT NULL,
            business_address character varying(255),
            employer character varying(255),
            occupation character varying(255)
        );
        ''',

        # Contributions table
        '''
        CREATE TABLE IF NOT EXISTS public.contributions (
            transaction_id character varying(20) NOT NULL,
            contributor_id integer NOT NULL,
            committee_id character varying(9) NOT NULL,
            contribution_receipt_amount numeric NOT NULL CHECK (contribution_receipt_amount >= 0),
            contribution_receipt_date date NOT NULL,
            sub_id character varying(20),
            year_cycle character(4) NOT NULL CHECK (year_cycle ~ '^[0-9]{4}$'),
            contributor_year character(4) NOT NULL CHECK (contributor_year ~ '^[0-9]{4}$')
        );
        '''
    ]

    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                for stmt in sql_statements:
                    cur.execute(stmt)
                    logging.info("Executed SQL statement successfully.")
            conn.commit()
            logging.info("All tables and sequences created successfully.")

    except Exception as e:
        logging.error(f"Failed to create tables: {e}")


if __name__ == "__main__":
    create_tables()
