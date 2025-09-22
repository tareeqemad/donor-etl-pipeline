import os
from connect import connect
from config import load_config


def execute_sql_from_file(conn, file_path):
    """Execute SQL commands from a .sql file"""
    try:
        # Open and read the SQL file
        with open(file_path, "r") as f:
            sql_script = f.read()

        # Create a cursor to interact with the database
        with conn.cursor() as cursor:
            # Execute the SQL script
            cursor.execute(sql_script)
            conn.commit()  # Commit the transaction
            print("SQL script executed successfully.")
        print("Database connection closed.")

    except Exception as error:
        print(f"Error executing SQL script: {error}")
        conn.rollback()  # In case of error, rollback


def main():
    # Load the database configuration
    config = load_config()

    # Establish the connection
    conn = connect(config)

    if conn is not None:
        sql_file_path = "createTables.sql"

        # Execute SQL commands from the file
        execute_sql_from_file(conn, sql_file_path)


if __name__ == "__main__":
    main()
