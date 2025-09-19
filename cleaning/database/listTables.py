from connect import connect, load_config

def list_tables():
    # Load config
    config = load_config()

    # Connect to DB
    conn = connect(config)

    if conn is None:
        print("Connection failed.")
        return

    try:
        with conn.cursor() as cur:
            # Query all table names from the public schema
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name;
            """)
            tables = cur.fetchall()

            print("Tables in database:")
            for table in tables:
                print(table[0])

    except Exception as e:
        print(f"Error while listing tables: {e}")

    finally:
        conn.close()


if __name__ == "__main__":
    list_tables()
