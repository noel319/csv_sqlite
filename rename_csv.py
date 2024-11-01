import pandas as pd
import spacy
import os
import chardet

# Load spaCy models for English and Russian
nlp_en = spacy.load("en_core_web_sm")
nlp_ru = spacy.load("ru_core_news_sm")

# Function to detect if a column contains names using both English and Russian models
def detect_name_spacy(col_data):
    name_count = 0
    entities = []

    for text in col_data.dropna().astype(str):
        doc_en = nlp_en(text)
        doc_ru = nlp_ru(text)        
        # Check for entities in both languages
        entities.extend([(ent.text, ent.label_) for ent in doc_en.ents])
        entities.extend([(ent.text, ent.label_) for ent in doc_ru.ents])
    # Count the detected entities for PERSON (which refers to names)
    for ent_text, ent_label in entities:
        if ent_label == 'PERSON':
            name_count += 1

    # Decide column type based on detected entity counts
    if name_count > 0:
        return 'name'
    else:
        return 'unknown'

# # Function to detect file encoding
# def detect_encoding(file_path):
#     with open(file_path, 'rb') as f:
#         rawdata = f.read(10)  # Read a portion of the file for detection
#     result = chardet.detect(rawdata)
#     return result['encoding']

def process_csv_files(folder_path):
    # Loop through all files in the directory
    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            # Construct the full file path
            file_path = os.path.join(folder_path, filename)
            
            try:
                # Automatically detect the file encoding
                detected_encoding = 'utf-8'                
                df = pd.read_csv(file_path, nrows=100, encoding=detected_encoding)                
                # Apply spaCy-based name detection to each column
                column_mappings = {}
                name_column_count = 0  # To track the number of name-related columns

                for col in df.columns:
                    column_type = detect_name_spacy(df[col])
                    
                    if column_type == 'name':                      
                        # If it's a name column, rename sequentially (name, name_1, name_2, etc.)
                        new_col_name = f"name" if name_column_count == 0 else f"name_{name_column_count}"
                        name_column_count += 1                        
                        column_mappings[col] = new_col_name
                    else:
                        column_mappings[col] = col  # Keep original name if it's not recognized

                # Rename columns based on the detected names (without modifying the data)
                df.rename(columns=column_mappings, inplace=True)
                # Save only the column headers to the same CSV file
                df.head(0).to_csv(file_path, index=False)                
                # Print a message for each file
                print(f"Columns have been renamed and saved for '{filename}' (only headers saved)")
            except Exception as e:
                print(f"Error processing {filename}: {str(e)}")
if __name__ == "__main__":
    folder_path = 'csv_output'  # Define the path to the folder containing the CSV files
    process_csv_files(folder_path)
