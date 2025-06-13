
import pandas as pd
import calendar

# Define file paths
file_path = "C:/Users/Sakshi/Documents/pharma-data-1 (1).csv"
cleaned_file_path = "cleaned_pharma_data.csv"

# Define chunk size for large files
chunk_size = 100000

# Define month mapping (January → 1, February → 2, etc.)
month_mapping = {month: index for index, month in enumerate(calendar.month_name) if month}

chunks = []

# Process dataset in chunks
for chunk in pd.read_csv(file_path, chunksize=chunk_size):
    # Rename columns: lowercase & replace spaces/hyphens with underscores
    chunk.columns = chunk.columns.str.lower().str.replace(" ", "_").str.replace("-", "_")

    # Convert Month names to numbers
    chunk["month"] = chunk["month"].map(month_mapping).astype(int)

    # Create a proper "date" column (assuming Day = 1 for consistency)
    chunk["date"] = pd.to_datetime(chunk["year"].astype(str) + "-" + chunk["month"].astype(str) + "-1")

    # Remove duplicate rows
    chunk.drop_duplicates(inplace=True)

    # Append cleaned chunk to list
    chunks.append(chunk)

# Concatenate all cleaned chunks
cleaned_df = pd.concat(chunks, ignore_index=True)

# Save cleaned data
cleaned_df.to_csv(cleaned_file_path, index=False)

print("✅ Data Cleaning & Column Renaming Complete. Saved as:", cleaned_file_path)
