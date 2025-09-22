import pandas as pd
import psycopg2
import logging
from config import load_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("insert_contributions_indiv.log"),
        logging.StreamHandler(),
    ],
)


def insert_from_csv(csv_path: str):
    """Insert individual contributions from CSV into PostgreSQL database."""

    config = load_config()

    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                logging.info("Connected to the database.")

                # Load CSV
                df = pd.read_csv(csv_path)
                logging.info(f"Loaded {len(df)} rows from {csv_path}")

                # Optional: fetch existing contributors to reduce repeated queries
                cur.execute("SELECT full_name, contributor_type, id FROM contributors")
                existing_contributors = {(r[0], r[1]): r[2] for r in cur.fetchall()}

                # Optional: fetch existing transaction_ids
                cur.execute("SELECT transaction_id FROM contributions")
                existing_transactions = set(r[0] for r in cur.fetchall())

                successful_inserts = 0
                failed_inserts = 0

                for idx, row in df.iterrows():
                    # Use a savepoint for each row so we can rollback individual failures
                    savepoint_name = f"sp_{idx}"

                    try:
                        cur.execute(f"SAVEPOINT {savepoint_name}")

                        # Build full_name
                        full_name = (
                            row.get("contributor_name")
                            or f"{row.get('contributor_first_name', '')} {row.get('contributor_last_name', '')}".strip()
                        )
                        key = (full_name, "individual")

                        # 1. Insert into contributors if not exists
                        if key in existing_contributors:
                            contributor_id = existing_contributors[key]
                        else:
                            cur.execute(
                                """
                                INSERT INTO contributors (full_name, contributor_type)
                                VALUES (%s, %s)
                                RETURNING id;
                                """,
                                (full_name, "individual"),
                            )
                            contributor_id = cur.fetchone()[0]
                            existing_contributors[key] = (
                                contributor_id  # update local cache
                            )

                        # 2. Insert into contributor_addresses (remove ON CONFLICT)
                        cur.execute(
                            """
                            INSERT INTO contributor_addresses
                            (contributor_id, street_1, street_2, city, state, zip, lat, lng)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                            """,
                            (
                                contributor_id,
                                row.get("contributor_street_1"),
                                row.get("contributor_street_2"),
                                row.get("contributor_city"),
                                row.get("contributor_state"),
                                row.get("contributor_zip"),
                                row.get("lat"),
                                row.get("lng"),
                            ),
                        )
                        # with .get : Returns None if column missing

                        # 3. Insert into contributor_business (remove ON CONFLICT)
                        cur.execute(
                            """
                            INSERT INTO contributor_business
                            (contributor_id, business_address, employer, occupation)
                            VALUES (%s, %s, %s, %s);
                            """,
                            (
                                contributor_id,
                                row.get("contributor_business_address"),
                                row.get("contributor_employer"),
                                row.get("contributor_occupation"),
                            ),
                        )

                        # 4. Insert into contributions (skip if transaction_id exists)
                        transaction_id = row["transaction_id"]
                        if transaction_id in existing_transactions:
                            logging.info(
                                f"Skipping already inserted transaction: {transaction_id}"
                            )
                            cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                            continue

                        cur.execute(
                            """
                            INSERT INTO contributions
                            (transaction_id, contributor_id, contribution_receipt_amount,
                             contribution_receipt_date, sub_id, year_cycle, contributor_year, committee_id)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                            """,
                            (
                                transaction_id,
                                contributor_id,
                                row["contribution_receipt_amount"],
                                row["contribution_receipt_date"],
                                row.get("sub_id"),
                                row.get("year_cycle"),
                                row.get("contributor_year"),
                                row.get("committee_id"),
                            ),
                        )
                        existing_transactions.add(transaction_id)

                        # Release the savepoint (commit this row)
                        cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                        successful_inserts += 1

                        if (idx + 1) % 1000 == 0:
                            logging.info(
                                f"Processed {idx+1}/{len(df)} rows ({successful_inserts} successful, {failed_inserts} failed)"
                            )

                    except Exception as row_err:
                        logging.error(f"Error inserting row {idx+1}: {row_err}")
                        # Rollback to savepoint (only this row)
                        cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                        cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                        failed_inserts += 1
                        continue

                # Commit all successful inserts
                conn.commit()
                logging.info(
                    f"Processing complete. {successful_inserts} successful inserts, {failed_inserts} failed inserts."
                )

    except Exception as e:
        logging.error(f"Database connection or insertion failed: {e}")


if __name__ == "__main__":
    insert_from_csv("data/clean_contrib_INDIV_enriched.csv")
