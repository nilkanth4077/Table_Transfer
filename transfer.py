import psycopg2

# Database connection parameters
params = {
    'user': 'postgres',
    'password': 'your_password',
    'host': 'localhost',
    'port': 5433
}

def transfer_tables(source_db, dest_db, conn_params):
    """Transfer tables from source_db to dest_db."""
    try:
        # Connect to source and destination databases
        src_conn = psycopg2.connect(database=source_db, **conn_params)
        dest_conn = psycopg2.connect(database=dest_db, **conn_params)
        
        src_conn.autocommit = True
        dest_conn.autocommit = True
        src_cur = src_conn.cursor()
        dest_cur = dest_conn.cursor()

        # Fetch the list of tables from the source database
        src_cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        tables = src_cur.fetchall()

        for table in tables:
            table_name = table[0]
            # Copy table schema
            src_cur.execute(f"SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_name = '{table_name}';")
            columns = src_cur.fetchall()
            create_table_sql = f"CREATE TABLE {table_name} ({', '.join([f'{column[0]} {column[1]}{"(" + str(column[2]) + ")" if column[2] else ""}' for column in columns])});"
            dest_cur.execute(create_table_sql)

            # Copy table data
            src_cur.execute(f"SELECT * FROM {table_name};")
            rows = src_cur.fetchall()
            if rows:
                insert_sql = f"INSERT INTO {table_name} VALUES ({', '.join(['%s' for _ in rows[0]])});"
                dest_cur.executemany(insert_sql, rows)

        src_cur.close()
        dest_cur.close()
        src_conn.close()
        dest_conn.close()
        print("Tables transferred successfully.")
    except Exception as error:
        print(f"Error: {error}")

# Main script execution
if __name__ == "__main__":
    # Transfer tables from Dev to QA
    transfer_tables("ems", "qa", params)
