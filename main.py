import sqlite3
import re
import os
# Usage
sqlite_folder = 'z:/'
TRACKING_FILE = 'rename.txt'
# Regex patterns for different types
name_pattern = re.compile(r'\b[AА][а-яА-ЯёЁ]+ [AА][а-яА-ЯёЁ]+\b')  # Basic name pattern
email_pattern = re.compile(r'([A-Za-z0-9]+[.-_])*[A-Za-z0-9]+@[A-Za-z0-9-]+(\.[A-Z|a-z]{2,})+')  # Simple email regex
phone_pattern = re.compile(r'\+?\d{1,3}[\s\-]?\(?\d{1,5}\)?[\s\-]?\d{1,4}[\s\-]?\d{1,4}[\s\-]?\d{1,4}')
# Function to detect column type using regex
def detect_column_type(column_data): 
    name_type = {'phone_number':0, 'email':0, 'full_name':0}
    # Check the first few rows of data to detect the type
    for item in column_data:
        if email_pattern.match(str(item)):
            name_type['email'] += 1        
        elif name_pattern.match(str(item)):
            name_type['full_name'] += 1
        elif detect_phone(str(item)):
            name_type['phone_number'] += 1
    col_name = next((key for key, value in name_type.items() if value > 50), None)
    return col_name

def detect_phone(column_data):
    if re.search(phone_pattern, str(column_data)):        
        normalized_number = re.sub(r'[^a-zA-Zа-яА-Я0-9\s]', '', str(column_data))
        if normalized_number.startswith('8') and len(normalized_number) == 11:
            return True
        elif normalized_number.startswith('7') and len(normalized_number) == 11:
            return True        
    return False
# Function to rename columns in the SQLite database
def rename_columns_based_on_data(db_path, table_name):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Get the column names from the specified table
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns_info = cursor.fetchall()
        column_names = [col[1] for col in columns_info]

        print("Original Columns:", column_names)        
        # Loop through each column and detect its type based on data
        renamed_columns = {}
        for column in column_names:
            # Query to get the first 20 rows of data for each column
            cursor.execute(f"SELECT {column} FROM {table_name} LIMIT 100")
            column_data = [row[0] for row in cursor.fetchall()]

            # Detect column type based on data using regex
            detected_type = detect_column_type(column_data)
            if detected_type:
                renamed_columns[column] = detected_type

        # If there are columns to rename, proceed
        if renamed_columns:
            print("Columns to Rename:", renamed_columns)

            # Start building the ALTER TABLE SQL commands
            alter_commands = []
            for old_name, new_name in renamed_columns.items():
                alter_commands.append(f"ALTER TABLE {table_name} RENAME COLUMN {old_name} TO {new_name}")

            # Execute each rename command
            for command in alter_commands:
                cursor.execute(command)
                print(f"Executed: {command}")

            # Commit the changes to the database
            conn.commit()
            print("Column renaming completed and saved to the database.")
        else:
            print("No columns matched the patterns.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the connection
        conn.close()
        with open(TRACKING_FILE, 'a', encoding="utf-8") as f:
            f.write(f"{db_path}\n")
def is_file_migrated(filename):
    """Check if the given filename is in the migrated files list."""
    if not os.path.exists(TRACKING_FILE):
        return False  

    with open(TRACKING_FILE, 'r', encoding="utf-8") as f:
        migrated_files = f.read().splitlines() 

    return filename in migrated_files 
if __name__ == "__main__": 
    sqlite_files = [os.path.join(sqlite_folder, file) for file in os.listdir(sqlite_folder) if file.endswith('.db')]
    table_name = 'main_content'      # The table you want to modify
    for db_path in sqlite_files:
        if is_file_migrated (db_path):
                continue
        rename_columns_based_on_data(db_path, table_name)
