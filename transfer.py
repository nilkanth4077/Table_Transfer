import psycopg2

# Database connection parameters
params = {
    'user': 'postgres',
    'password': 'your_password',
    'host': 'localhost',
    'port': 5433
}

def transfer_tables(source_db, dest_db, conn_params):
    """Transfer tables with constraints from source_db to dest_db."""
    try:
        # Connect to source and destination databases
        src_conn = psycopg2.connect(database=source_db, **conn_params)
        dest_conn = psycopg2.connect(database=dest_db, **conn_params)
        
        # Set autocommit to True for both connections
        src_conn.autocommit = True
        dest_conn.autocommit = True
        
        # Create cursors for both connections
        src_cur = src_conn.cursor()
        dest_cur = dest_conn.cursor()

        # Fetch the list of tables from the source database
        src_cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        tables = src_cur.fetchall()

        # Iterate over each table in the source database
        for table in tables:
            table_name = table[0]
            
            # Copy table schema from source to destination
            src_cur.execute(f"SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale FROM information_schema.columns WHERE table_name = '{table_name}';")
            columns = src_cur.fetchall()
            create_table_columns = []

            for column in columns:
                column_name = column[0]
                data_type = column[1]
                max_length = column[2]
                numeric_precision = column[3]
                numeric_scale = column[4]

                # Construct column definition with length/precision
                if data_type in ['character varying', 'varchar', 'character', 'char']:
                    if max_length:
                        col_def = f"{column_name} {data_type}({max_length})"
                    else:
                        col_def = f"{column_name} {data_type}"
                elif data_type in ['numeric', 'decimal']:
                    if numeric_precision and numeric_scale:
                        col_def = f"{column_name} {data_type}({numeric_precision},{numeric_scale})"
                    elif numeric_precision:
                        col_def = f"{column_name} {data_type}({numeric_precision})"
                    else:
                        col_def = f"{column_name} {data_type}"
                else:
                    col_def = f"{column_name} {data_type}"

                create_table_columns.append(col_def)

            create_table_sql = f"CREATE TABLE {table_name} ({', '.join(create_table_columns)});"
            dest_cur.execute(create_table_sql)

            # Copy primary key constraint from source to destination
            src_cur.execute(f"SELECT conname, pg_get_constraintdef(con.oid) FROM pg_constraint con JOIN pg_class rel ON rel.oid = con.conrelid WHERE rel.relname = '{table_name}' AND con.contype = 'p';")
            pk_constraint = src_cur.fetchone()
            if pk_constraint:
                dest_cur.execute(f"ALTER TABLE {table_name} ADD CONSTRAINT {pk_constraint[0]} {pk_constraint[1].replace('ALTER TABLE ONLY','')};")

            # Copy foreign key constraints from source to destination
            src_cur.execute(f"SELECT conname, pg_get_constraintdef(con.oid) FROM pg_constraint con JOIN pg_class rel ON rel.oid = con.conrelid WHERE rel.relname = '{table_name}' AND con.contype = 'f';")
            fk_constraints = src_cur.fetchall()
            for fk_constraint in fk_constraints:
                dest_cur.execute(f"ALTER TABLE {table_name} ADD CONSTRAINT {fk_constraint[0]} {fk_constraint[1].replace('ALTER TABLE ONLY','')};")

            # Copy not null constraints from source to destination
            src_cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' AND is_nullable = 'NO';")
            not_null_columns = src_cur.fetchall()
            for column in not_null_columns:
                dest_cur.execute(f"ALTER TABLE {table_name} ALTER COLUMN {column[0]} SET NOT NULL;")

            # Copy table data from source to destination
            src_cur.execute(f"SELECT * FROM {table_name};")
            rows = src_cur.fetchall()
            if rows:
                insert_sql = f"INSERT INTO {table_name} VALUES ({', '.join(['%s' for _ in rows[0]])});"
                dest_cur.executemany(insert_sql, rows)

        # Close cursors and connections
        src_cur.close()
        dest_cur.close()
        src_conn.close()
        dest_conn.close()
        
        print("Tables transferred successfully.")
    
    except Exception as error:
        print(f"Error: {error}")

# Main script execution
if __name__ == "__main__":
    # Transfer tables from 'source_db' to 'dest_db'
    transfer_tables("ems", "qa", params)
