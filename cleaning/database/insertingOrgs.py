import pandas as pd
import psycopg2
import logging
from config import load_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("insert_contributions_orgs.log"), logging.StreamHandler()]
)

def insert_from_csv(csv_path: str):
    """Insert organization contributions from CSV into PostgreSQL database."""

    # Load config
    config = load_config()

    try:
        # Connect to DB
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                logging.info("Connected to the database.")

                # Read CSV
                df = pd.read_csv(csv_path)
                logging.info(f"Loaded {len(df)} rows from {csv_path}")

                for idx, row in df.iterrows():
                    try:
                        # 1. Insert into contributors
                        cur.execute(
                            """
                            INSERT INTO contributors (full_name, contributor_type)
                            VALUES (%s, %s)
                            RETURNING id;
                            """,
                            (row["contributor_name"], "organization")
                        )
                        contributor_id = cur.fetchone()[0]

                        # 2. Insert into contributor_addresses
                        cur.execute(
                            """
                            INSERT INTO contributor_addresses
                            (contributor_id, street_1, street_2, city, state, zip, lat, lng)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                            """,
                            (
                                contributor_id,
                                row["contributor_street_1"],
                                row.get("contributor_street_2"),
                                row["contributor_city"],
                                row["contributor_state"],
                                row["contributor_zip"],
                                row["lat"],
                                row["lng"]
                            )
                        )

                        # 3. Insert into contributor_business
                        cur.execute(
                            """
                            INSERT INTO contributor_business
                            (contributor_id, business_address, employer, occupation)
                            VALUES (%s, %s, %s, %s);
                            """,
                            (
                                contributor_id,
                                None,  # No "business_address" in CSV
                                row.get("contributor_employer"),
                                row.get("contributor_occupation")
                                
                            )
                        )

                        # 4. Insert into contributions
                        cur.execute(
                            """
                            INSERT INTO contributions
                            (transaction_id, contributor_id, contribution_receipt_amount,
                             contribution_receipt_date, sub_id, year_cycle, contributor_year, committee_id)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                            """,
                            (
                                row["transaction_id"],
                                contributor_id,
                                row["contribution_receipt_amount"],
                                row["contribution_receipt_date"],
                                row["sub_id"],
                                row["year_cycle"],
                                row["contributor_year"],
                                row["committee_id"]
                            )
                        )

                        logging.info(f"Inserted row {idx+1}/{len(df)} for contributor {row['contributor_name']}")

                    except Exception as row_err:
                        logging.error(f"Error inserting row {idx+1}: {row_err}")
                        conn.rollback()  # rollback failed row
                        continue

                # Commit all successful inserts
                conn.commit()
                logging.info("All valid rows committed to database.")

    except Exception as e:
        logging.error(f"Database connection or insertion failed: {e}")


if __name__ == "__main__":
    insert_from_csv("data/clean_contributionsBYorganizations.csv")
