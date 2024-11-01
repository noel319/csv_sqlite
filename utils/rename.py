import re
def rename_func(df):   
    new_column_names = []
    col_count = 0
    for column in df.columns:
        col_count += 1
        new_column_names.append(f"col_{col_count}")
    # Rename DataFrame columns    
    return new_column_names