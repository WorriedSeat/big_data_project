import os
import pandas as pd
import psycopg2 as psql
from pprint import pprint

def build_projectdb():
    print("Starting building database")

    # Getting password
    secrets_file = os.path.join("secrets", ".psql.pass")
    if not os.path.exists(secrets_file):
        raise FileNotFoundError(f"File {secrets_file} not found! Create secrets/.psql.pass")
    with open(secrets_file, "r") as f:
        password = f.read().strip()

    # Connecting to db
    conn_string = (
        "host=hadoop-04.uni.innopolis.ru "
        "port=5432 "
        "user=team14 "
        "dbname=team14_projectdb "
        f"password={password}"
    )

    with psql.connect(conn_string) as conn:
        cur = conn.cursor()
        print("  Connected to PostgreSQL")

        # Creating tables
        print("  Creating tables...")
        with open("sql/create_tables.sql", "r") as f:
            create_sql = f.read()
            cur.execute(create_sql)
        conn.commit()

        # Ingesting data
        print("  Ingesting data in tables...")

        with open("sql/import_data.sql", "r") as f:
            sql_text = f.read()

        lines = sql_text.splitlines()
        clean_commands = []
        current_cmd = []

        for line in lines:
            stripped = line.strip()
            if not stripped or stripped.startswith("--"):
                continue
            current_cmd.append(line)
            if stripped.endswith(";"):
                clean_commands.append("\n".join(current_cmd).strip())
                current_cmd = []

        if len(clean_commands) != 3:
            raise ValueError(f"Waited for 3 COPY-commands, found {len(clean_commands)}")

        # airlines
        with open("data/airlines.csv", "r") as f:
            cur.copy_expert(clean_commands[0], f)
        print("\tairlines table ready")

        # airports
        with open("data/airports.csv", "r") as f:
            cur.copy_expert(clean_commands[1], f)
        print("\tairports table ready")

        # flights
        with open("data/flights.csv", "r") as f:
            cur.copy_expert(clean_commands[2], f)
        print("\tflights table ready")

        conn.commit()

        # Running post processing step
        print("Executing post processing...")
        with open("sql/post_process.sql", "r") as f:
            cur.execute(f.read())
        conn.commit()
        print("All tables loaded successfully!")

        # Running test queries
        with open("sql/test_database.sql", "r") as f:
            test_commands = f.readlines()

        for i, command in enumerate(test_commands, 1):
            command = command.strip()
            if command and not command.startswith("--"):
                cur.execute(command)
                result = cur.fetchall()
                pprint(result)
                print("-" * 80)

        print("Successfully built database")

if __name__ == "__main__":
    build_projectdb()