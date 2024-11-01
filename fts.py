import os
import pandas as pd
import sqlite3
import chardet

MIGRATED_FILES_LOG = 'migrated_files.txt'

def get_max_columns(folder_path):
    """Determine the maximum number of columns across all CSV files in the folder."""
    max_columns = 10
    for csv_file in os.listdir(folder_path):
        if csv_file.endswith('.csv'):
            file_path = os.path.join(folder_path, csv_file)
            try:
                df = pd.read_csv(file_path, nrows=1, encoding='utf-8')
                max_columns = max(max_columns, len(df.columns))
            except UnicodeDecodeError:
                encoding = 'ISO-8859-1'  # Fallback encoding
                df = pd.read_csv(file_path, nrows=1, encoding=encoding)           
                max_columns = max(max_columns, len(df.columns))            
    return max_columns

def create_sqlite_table(sqlite_db, table_name, column_count):
    """Create an SQLite table with a specified number of columns named col_1, col_2, etc."""
    conn = sqlite3.connect(sqlite_db)
    cursor = conn.cursor()

    # Define column names based on the maximum column count
    columns_definition = ", ".join([f'col_{i+1} TEXT' for i in range(column_count)])    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        {columns_definition}
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    conn.close()

def create_fts5_table(sqlite_db, fts_table_name, source_table_name, column_count):
    """Create an FTS5 virtual table linked to the main table."""
    conn = sqlite3.connect(sqlite_db)
    cursor = conn.cursor()    
    # Define column names for FTS5
    fts_columns_definition = ", ".join([f'col_{i+1}' for i in range(column_count)])    
    create_fts_table_query = f"""
    CREATE VIRTUAL TABLE IF NOT EXISTS {fts_table_name} USING fts5(
        {fts_columns_definition},
        content='{source_table_name}',
        content_rowid='id'
    );
    """
    cursor.execute(create_fts_table_query)
    conn.commit()
    conn.close()

def migrate_csv_to_sqlite(csv_file, sqlite_db, table_name, fts_table_name, column_count, chunk_size=5000):
    conn = sqlite3.connect(sqlite_db)
    cursor = conn.cursor()

    # Detect encoding
    with open(csv_file, 'rb') as f:
        result = chardet.detect(f.read(1000))
        encoding = result['encoding']   
        
    # Process CSV in chunks
    for chunk in pd.read_csv(csv_file, chunksize=chunk_size, encoding=encoding,on_bad_lines='skip'):
        # Ensure the chunk has the correct number of columns
        current_columns = len(chunk.columns)
        if current_columns < column_count:
            additional_columns = pd.DataFrame({f'col_{i+1}': [None] * len(chunk) for i in range(current_columns, column_count)})
            chunk = pd.concat([chunk, additional_columns], axis=1)
        chunk.columns = [f'col_{i+1}' for i in range(column_count)]
        # Insert data into SQLite main content table
        chunk.to_sql(table_name, conn, if_exists='append', index=False)

        # Insert into FTS5 table
        column_list = ", ".join([f'col_{i+1}' for i in range(column_count)])
        cursor.execute(f"""
            INSERT INTO {fts_table_name} ({column_list}) 
            SELECT {column_list} FROM {table_name}
            WHERE id > (SELECT IFNULL(MAX(rowid), 0) FROM {fts_table_name});
        """)
        print(f"Inserted data into FTS5 table.")

    conn.commit()
    conn.close()
    print(f"Finished processing {csv_file}")

    # Log the migrated file
    log_migrated_file(csv_file)

def log_migrated_file(csv_file):    
    with open(MIGRATED_FILES_LOG, 'a', encoding='utf-8') as log_file:
        log_file.write(f"{csv_file}\n")

def get_migrated_files():    
    if os.path.exists(MIGRATED_FILES_LOG):
        with open(MIGRATED_FILES_LOG, 'r', encoding='utf-8') as log_file:
            return set(line.strip() for line in log_file)
    return set()

def process_csv_file(file_path, sqlite_db, table_name, fts_table_name, column_count):
    print(f"Starting process for: {file_path}")
    migrate_csv_to_sqlite(file_path, sqlite_db, table_name, fts_table_name, column_count)

def process_csv_folder(folder_path, sqlite_db, table_name, fts_table_name):
    # Determine the maximum column count across all CSV files
    max_columns = get_max_columns(folder_path)
    print(f"Maximum columns found: {max_columns}")

    # Create SQLite main and FTS5 tables with the max column count
    create_sqlite_table(sqlite_db, table_name, max_columns)
    create_fts5_table(sqlite_db, fts_table_name, table_name, max_columns)

    # Get list of CSV files that need processing
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    if not csv_files:
        print("No CSV files found in the specified folder.")
        return
    migrated_files = get_migrated_files()
    files_to_process = [os.path.join(folder_path, f) for f in csv_files if os.path.join(folder_path, f) not in migrated_files]

    if not files_to_process:
        print("All files have already been migrated.")
        return

    # Process each CSV file
    for file_path in files_to_process:
        process_csv_file(file_path, sqlite_db, table_name, fts_table_name, max_columns)
    print("All files have been processed.")

if __name__ == "__main__":
    csv_folder = 'csv_output'  # Folder containing the CSV files
    sqlite_db = 'meta.db'
    table_name = 'main_content'  # Main content table for CSV data
    fts_table_name = 'main'  # FTS5 table for full-text search
    process_csv_folder(csv_folder, sqlite_db, table_name, fts_table_name)
