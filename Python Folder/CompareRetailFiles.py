# This script compares two retail files and outputs the differences.
# It assumes both files have a common key column to identify rows.
# I didn't include any specific error handling or logging for simplicity.
# You can use your own paths for CurrentFile and PreviousFile.

import pandas as pd
from pathlib import Path

# Define your input files
CurrentFile = Path("Put current path with your current file.")
PreviousFile = Path("Put current path with your previous file.")

# Define the key columns that uniquely identify a row (adjust as needed)
key_columns = ["upc_nbr"]  # <-- Change this if needed

# Load the CSVs
Current_df = pd.read_csv(CurrentFile)
Previous_df = pd.read_csv(PreviousFile)

# Set the key columns as index for comparison
Current_df.set_index(key_columns, inplace=True)
Previous_df.set_index(key_columns, inplace=True)

# Align both DataFrames on the keys and keep only differing rows
# Keep only keys that exist in both, then compare the rest of the row
shared_keys = Current_df.index.intersection(Previous_df.index)
changed_rows = Current_df.loc[shared_keys].compare(Previous_df.loc[shared_keys])

# Reset index to get key columns back as columns
changed_rows.reset_index(inplace=True)

# Determine output path in same folder as current file
output_path = CurrentFile.parent / "RetailFileDiff.csv"
changed_rows.to_csv(output_path, index=False)

print(f"{len(changed_rows)} changed rows written to: {output_path}")
