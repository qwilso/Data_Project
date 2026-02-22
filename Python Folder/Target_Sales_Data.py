import pandas as pd
import pyodbc
import os
import shutil
from dotenv import load_dotenv
from datetime import datetime

# --- 1. PATH SETUP (The "Bulletproof" Way) ---
# This finds your folders regardless of where you run the terminal from
base_path = os.path.dirname(os.path.abspath(__file__)) # The 'Python Folder'
project_root = os.path.dirname(base_path)              # The 'Data_Project' folder

# Define folder paths
SOURCE_DIR = os.path.join(project_root, 'ETL_Folder', 'Source_Folder')
TARGET_DIR = os.path.join(project_root, 'ETL_Folder', 'Target_Folder')
ARCHIVE_DIR = os.path.join(project_root, 'ETL_Folder', 'Archive_Folder')

# --- 2. LOAD ENVIRONMENT VARIABLES ---
# We point load_dotenv directly to the file in the current script's folder
load_dotenv(os.path.join(base_path, '.env'))

DB_SERVER = os.getenv('DB_SERVER')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')

def run_etl():
    print(f"Starting ETL Pipeline at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Looking for files in: {SOURCE_DIR}")

    conn = None
    try:
        # --- 3. DATABASE CONNECTION ---
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={DB_SERVER},1433;"
            f"DATABASE={DB_NAME};"
            f"UID={DB_USER};"
            f"PWD={DB_PASS};"
            "Encrypt=yes;"
            "TrustServerCertificate=yes;" # Required for Docker/Local Dev
            "ConnectTimeout=30;"
        )
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        print("✅ Successfully connected to SQL Server.")

        # --- 4. EXTRACTION ---
        files = [f for f in os.listdir(SOURCE_DIR) if f.endswith('.csv')]
        
        if not files:
            print("Empty-handed: No CSV files found in Source Folder.")
            return

        for filename in files:
            file_path = os.path.join(SOURCE_DIR, filename)
            print(f"Extracting: {filename}")
            
            # Load data
            df = pd.read_csv(file_path)

            # --- 5. TRANSFORMATION (Example: Clean names and add a timestamp) ---
            # Make sure your CSV columns match these, or adjust as needed!
            df.columns = [c.strip().replace(' ', '_').lower() for c in df.columns]
            df['processed_at'] = datetime.now()

            # --- 6. LOADING ---
            # Replace 'Your_Table_Name' with the actual table in your DB
            # We use a simple loop here for your 2,000 rows
            for index, row in df.iterrows():
                # Example SQL - adjust the column names to match your table!
                cursor.execute("""
                    INSERT INTO Your_Table_Name (column1, column2, processed_at)
                    VALUES (?, ?, ?)
                """, row['column1'], row['column2'], row['processed_at'])
            
            conn.commit()
            print(f"Loaded {len(df)} rows into the database.")

            # --- 7. ARCHIVING ---
            # Move the file so it doesn't get processed again next time
            if not os.path.exists(ARCHIVE_DIR):
                os.makedirs(ARCHIVE_DIR)
            
            shutil.move(file_path, os.path.join(ARCHIVE_DIR, filename))
            print(f"Moved {filename} to Archive_Folder.")

    except Exception as e:
        print(f"Pipeline Error: {e}")
    
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    run_etl()