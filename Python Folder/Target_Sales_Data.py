import pandas as pd
import pyodbc
import os
import shutil
from datetime import datetime
from dotenv import load_dotenv

# --- 1. INITIALIZATION & SECURITY ---
# Load environment variables from the hidden .env file
load_dotenv()

DB_SERVER = os.getenv('DB_SERVER')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')

# Folder Paths
SOURCE_DIR = './Source Folder/'
TARGET_DIR = './Target Folder/'
ARCHIVE_DIR = './Archive_Folder/'

# Ensure necessary directories exist locally
os.makedirs(TARGET_DIR, exist_ok=True)
os.makedirs(ARCHIVE_DIR, exist_ok=True)

def run_etl_pipeline():
    print(f"Starting ETL Pipeline at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # --- 2. DATABASE CONNECTION ---
    # Note: Using ODBC Driver 18 for macOS compatibility
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_NAME};"
        f"UID={DB_USER};"
        f"PWD={DB_PASS};"
        "Encrypt=no;"
    )
    
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # --- 3. EXTRACTION (File Pattern Matching) ---
        files = [f for f in os.listdir(SOURCE_DIR) if f.endswith('.csv') and "Sales_Data" in f]

        if not files:
            print(f"No new 'Sales_Data' files found in {SOURCE_DIR}.")
            return

        for file_name in files:
            file_path = os.path.join(SOURCE_DIR, file_name)
            print(f"Extracting: {file_name}")

            # --- 4. TRANSFORMATION (Business Logic) ---
            df = pd.read_csv(file_path)
            
            # Standardization: Concatenate names and fix casing
            df['FullName'] = df['First_Name'].str.strip().str.title() + ' ' + df['Last_Name'].str.strip().str.title()
            
            # Data Cleansing: Remove currency symbols and convert to float
            df['CleanAmount'] = df['Raw_Amount'].replace('[\$,]', '', regex=True).astype(float)
            
            # Business Logic: Calculate Tax (Example: 10%)
            df['Tax_Amount'] = df['CleanAmount'] * 0.10

            # --- 5. LOADING (Multi-Destination) ---
            # Destination A: SQL Server (Record Keeping)
            for _, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO FIN_SALES_REPORTING_BASE (TRANS_ID, CUSTOMER_FULL_NAME, NET_SALES_AMT, TAX_AMT)
                    VALUES (?, ?, ?, ?)
                """, row['Order_ID'], row['FullName'], row['CleanAmount'], row['Tax_Amount'])
            
            conn.commit()
            print(f"Successfully loaded {len(df)} rows to SQL Server.")

            # Destination B: Target Folder (Business Reporting)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            df.to_csv(os.path.join(TARGET_DIR, f"Cleaned_Financials_{timestamp}.csv"), index=False)

            # --- 6. ARCHIVING (Audit Trail) ---
            shutil.move(file_path, os.path.join(ARCHIVE_DIR, f"Processed_{timestamp}_{file_name}"))
            print(f"Original file moved to Archive Folder.")

    except Exception as e:
        print(f"Pipeline Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    run_etl_pipeline()