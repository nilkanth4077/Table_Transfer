import psycopg2

# Database connection parameters
params = {
    'user': 'postgres',
    'password': 'your_password',
    'host': 'localhost',
    'port': 5433
}

# Check if a table exists in the database
def check_table_exists(cursor, table_name):
    
    cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{table_name}');")
    return cursor.fetchone()[0]

# Modify table if certain columns not present then adds to dest_table
def alter_table_schema(cursor, table_name, columns):
    
    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';")
    existing_columns = [col[0] for col in cursor.fetchall()]

    for column in columns:
        column_name = column[0]
        data_type = column[1]
        max_length = column[2]
        numeric_precision = column[3]
        numeric_scale = column[4]

        # Add column if it doesn't exist
        if column_name not in existing_columns:
            alter_sql = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {data_type}"
            if data_type in ['character varying', 'varchar', 'character', 'char']:
                if max_length:
                    alter_sql += f"({max_length})"
            elif data_type in ['numeric', 'decimal']:
                if numeric_precision and numeric_scale:
                    alter_sql += f"({numeric_precision},{numeric_scale})"
                elif numeric_precision:
                    alter_sql += f"({numeric_precision})"
            cursor.execute(alter_sql)

# Data transfer function
def transfer_data(cursor_src, cursor_dest, table_name):
    """Transfer data from source table to destination table."""
    cursor_src.execute(f"SELECT * FROM {table_name};")
    rows = cursor_src.fetchall()
    
    if rows:
        insert_sql = f"INSERT INTO {table_name} VALUES ({', '.join(['%s' for _ in rows[0]])});"
        cursor_dest.executemany(insert_sql, rows)

# Main function
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
            
            # Check if the table already exists in the destination database
            table_exists = check_table_exists(dest_cur, table_name)

            if table_exists:
                # If table exists, alter its schema (columns)
                src_cur.execute(f"SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale FROM information_schema.columns WHERE table_name = '{table_name}';")
                columns = src_cur.fetchall()
                alter_table_schema(dest_cur, table_name, columns)

            else:
                # If table does not exist, create it with full schema and data
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

            # Transfer data from source to destination
            transfer_data(src_cur, dest_cur, table_name)

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
    # Transfer tables from 'jwt' (Dev) to 'qa' (QA)
    transfer_tables("dev", "try", params)
