import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('metadata.db')
cursor = conn.cursor()

# Rename the table
cursor.execute("ALTER TABLE main_content RENAME TO data;")

# Commit the changes and close the connection
conn.commit()
conn.close()

print("Table renamed successfully!")
