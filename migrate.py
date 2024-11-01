import os
import pandas as pd
import sqlite3
from utils.rename import rename_func
import chardet
from io import TextIOWrapper
MIGRATED_FILES_LOG = 'migrated_files.txt'
def migrate_csv_to_sqlite(csv_file, sqlite_db, table_name, chunk_size=5000):
    # Connect to SQLite database (create if not exists)
    conn = sqlite3.connect(sqlite_db)
    cursor = conn.cursor()
    
    # Detect encoding
    with open(csv_file, 'rb') as f:
        result = chardet.detect(f.read(1000))
        encoding = result['encoding']

    # Check if the table exists, if not create it
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
    if cursor.fetchone() is None:
        print(f"Table '{table_name}' does not exist. Creating table...")
        create_table_query = f"""
        CREATE TABLE {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,            
            email TEXT,
            phone TEXT,
            full_name TEXT
        );
        """
        cursor.execute(create_table_query)
        conn.commit()  # Commit after table creation

    # Read the first 20 rows to get column names
    try:
        df = pd.read_csv(csv_file, skiprows=range(5), nrows=25, encoding=encoding)
    except UnicodeDecodeError:
        print(f"Failed with encoding {encoding}. Retrying with 'ISO-8859-1'.")
        encoding = 'ISO-8859-1'  # Fallback encoding
        df = pd.read_csv(csv_file, skiprows=range(5), nrows=25, encoding=encoding)

    column_names = rename_func(df)
    print("Renamed columns:", column_names)
    with open(csv_file, "rb") as f:
        text_file = TextIOWrapper(f, encoding="utf-8", errors="replace")
    # Read CSV in chunks with error handling
        for chunk in pd.read_csv(text_file, chunksize=chunk_size, on_bad_lines='skip'):
            print("Processing chunk columns:", chunk.columns)        
            # Get existing columns in the SQLite table
            existing_columns = [col[1] for col in cursor.execute(f"PRAGMA table_info({table_name})").fetchall()]
            print(column_names)
            chunk.columns = column_names            
            # Add any missing columns to the SQLite table
            for column in column_names:
                # Ensure to quote the column name
                quoted_column = f'"{column}"'
                if column not in existing_columns:
                    try:
                        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {quoted_column} TEXT")
                        print(f"Added column: {quoted_column}")
                    except sqlite3.OperationalError as e:
                        print(f"Error adding column {quoted_column}: {e}")
            # Rename chunk columns
            chunk.columns = column_names  # Rename chunk columns to match the final structure
            # Insert data into SQLite table
            chunk.to_sql(table_name, conn, if_exists='append', index=False)
    # Commit and close the connection
    conn.commit()
    conn.close()   
    with open(MIGRATED_FILES_LOG, 'a', encoding='utf-8') as log_file:
        log_file.write(f"{csv_file}\n")

def is_file_migrated(filename):
    """Check if the given filename is in the migrated files list."""
    if not os.path.exists(MIGRATED_FILES_LOG):
        return False  

    with open(MIGRATED_FILES_LOG, 'r', encoding="utf-8") as f:
        migrated_files = f.read().splitlines() 

    return filename in migrated_files 

def process_csv_folder(folder_path, sqlite_db, table_name, chunk_size=500):
    # Get list of all CSV files in the folder
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    if not csv_files:
        print("No CSV files found in the specified folder.")
        return
    # Process each CSV file
    for csv_file in csv_files:
        file_path = os.path.join(folder_path, csv_file)
        if is_file_migrated(file_path):
            continue
        print(f"Processing file: {file_path}")
        migrate_csv_to_sqlite(file_path, sqlite_db, table_name, chunk_size)
if __name__ == "__main__":
    csv_folder = 'csv_output'  # Folder containing the CSV files
    sqlite_db = 'metadata.db'
    table_name = 'main_content'
    process_csv_folder(csv_folder, sqlite_db, table_name)