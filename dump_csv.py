import os
import sqlite3
import csv
from multiprocessing import Pool, cpu_count

# Define the directory where all the .db files are stored
db_directory = './db'
chunk_size = 3000  # Number of rows to fetch per chunk
log_file = 'dumped_databases.txt'  # File to save the paths of successfully dumped databases
csv_directory = './csv_output'
# Ensure the CSV output directory exists
if not os.path.exists(csv_directory):
    os.makedirs(csv_directory)
# Function to write a chunk of rows to CSV
def write_chunk_to_csv(args):
    chunk, csv_file_path, column_names, mode = args
    
    with open(csv_file_path, mode, newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)
        
        # Write the headers only if it's the first chunk
        if mode == 'w':
            csv_writer.writerow(column_names)  
        
        # Write the rows
        csv_writer.writerows(chunk)

# Function to dump main table to CSV if it has at least 100,000 rows, with chunking and multiprocessing
def dump_table_if_large(db_path):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Check if the main table has 100,000 or more rows
    cursor.execute("SELECT COUNT(*) FROM main")  # Replace 'main' with the actual table name if different
    row_count = cursor.fetchone()[0]

    if row_count <= 200000:
        # Query the column names
        cursor.execute("PRAGMA table_info(main)")  # Get column information from the 'main' table
        column_names = [info[1] for info in cursor.fetchall()]

        # Generate the CSV file name based on the database file name
        csv_file_name = os.path.splitext(os.path.basename(db_path))[0] + '.csv'
        csv_file_path = os.path.join(csv_directory, csv_file_name)

        print(f"Dumping {csv_file_name} with {row_count} rows.")

        # Use multiprocessing to handle writing chunks
        pool = Pool(cpu_count())  # Create a pool of workers
        
        # Query and fetch the rows in chunks
        cursor.execute("SELECT * FROM main")
        chunk_counter = 0
        while True:
            chunk = cursor.fetchmany(chunk_size)
            if not chunk:
                break
            
            mode = 'w' if chunk_counter == 0 else 'a'  # Write mode for the first chunk, append mode for the rest
            pool.apply_async(write_chunk_to_csv, [(chunk, csv_file_path, column_names, mode)])
            chunk_counter += 1

        pool.close()
        pool.join()
        
        print(f"{csv_file_name} created in {csv_directory}.")

        # Log the database path to the log file
        with open(log_file, 'a',encoding='utf-8') as log:
            log.write(f"{db_path}\n")

    else:
        print(f"{db_path} has less than 200,000 rows. Skipping.")
    
    # Close the database connection
    conn.close()

def is_file_migrated(filename):
    """Check if the given filename is in the migrated files list."""
    if not os.path.exists(log_file):
        return False 
# Loop through all .db files in the /db folder
if __name__ == '__main__':
    db_files = [f for f in os.listdir(db_directory) if f.endswith('.db')]
    tasks=[]
    for db_file in db_files:
        if is_file_migrated (db_file):
            continue
        tasks.append(db_file)            
    for db_file in tasks:
        db_path = os.path.join(db_directory, db_file)
        dump_table_if_large(db_path)
