import os
import pandas as pd
import sqlite3
from utils.rename import rename_func
import chardet
from io import TextIOWrapper
from multiprocessing import Pool, current_process
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
    print(f"[{current_process().name}] Renamed columns:", column_names)
    
    with open(csv_file, "rb") as f:
        text_file = TextIOWrapper(f, encoding="utf-8", errors="replace")
        # Read CSV in chunks with error handling
        for chunk in pd.read_csv(text_file, chunksize=chunk_size, on_bad_lines='skip'):
            print(f"[{current_process().name}] Processing chunk columns:", chunk.columns)        
            # Get existing columns in the SQLite table
            existing_columns = [col[1] for col in cursor.execute(f"PRAGMA table_info({table_name})").fetchall()]
            print(f"[{current_process().name}] Existing columns:", existing_columns)            
            chunk.columns = column_names            
            # Add any missing columns to the SQLite table
            for column in column_names:
                quoted_column = f'"{column}"'
                if column not in existing_columns:
                    try:
                        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {quoted_column} TEXT")
                        print(f"[{current_process().name}] Added column: {quoted_column}")
                    except sqlite3.OperationalError as e:
                        print(f"[{current_process().name}] Error adding column {quoted_column}: {e}")

            # Insert data into SQLite table
            chunk.to_sql(table_name, conn, if_exists='append', index=False)
    
    # Commit and close the connection
    conn.commit()
    conn.close()
    print(f"[{current_process().name}] Finished processing {csv_file}")
    # Log the migrated file
    log_migrated_file(csv_file)

def log_migrated_file(csv_file):    
    with open(MIGRATED_FILES_LOG, 'a', encoding='utf-8') as log_file:
        log_file.write(f"{csv_file}\n")

def process_csv_file(file_path, sqlite_db, table_name):
    print(f"Starting process for: {file_path}")
    migrate_csv_to_sqlite(file_path, sqlite_db, table_name)


def get_migrated_files():    
    if os.path.exists(MIGRATED_FILES_LOG):
        with open(MIGRATED_FILES_LOG, 'r',encoding='utf-8') as log_file:
            return set(line.strip() for line in log_file)
    return set()

def process_csv_folder(folder_path, sqlite_db, table_name):
    # Get list of all CSV files in the folder
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    if not csv_files:
        print("No CSV files found in the specified folder.")
        return
    migrated_files = get_migrated_files()   
    files_to_process = [os.path.join(folder_path, f) for f in csv_files if os.path.join(folder_path, f) not in migrated_files]
    # Process each CSV file
    if not files_to_process:
        print("All files have already been migrated.")
        return

    with Pool(processes=2) as pool:
        pool.starmap(process_csv_file, [(file_path, sqlite_db, table_name) for file_path in files_to_process])
    
    print("All files have been processed.")

if __name__ == "__main__":
    csv_folder = 'csv_output'  # Folder containing the CSV files
    sqlite_db = 'metadata.db'
    table_name = 'main_content'
    process_csv_folder(csv_folder, sqlite_db, table_name)
