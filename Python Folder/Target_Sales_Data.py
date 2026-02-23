import pandas as pd
import pyodbc
import os
import shutil
from dotenv import load_dotenv
from datetime import datetime

# --- 1. PATH SETUP ---
# Finds folders inside 'ETL_Folder' relative to this script
base_path = os.path.dirname(os.path.abspath(__file__)) # 'Python Folder'
project_root = os.path.dirname(base_path)              # 'Data_Project'

# Path to the ETL_Folder contents
SOURCE_DIR = os.path.join(project_root, 'ETL_Folder', 'Source_Folder')
TARGET_DIR = os.path.join(project_root, 'ETL_Folder', 'Target_Folder')
ARCHIVE_DIR = os.path.join(project_root, 'ETL_Folder', 'Archive_Folder')

# --- 2. LOAD ENVIRONMENT VARIABLES ---
load_dotenv(os.path.join(base_path, '.env'))

DB_SERVER = os.getenv('DB_SERVER')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')

def run_etl():
    print(f"🚀 Starting ETL Pipeline: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    conn = None
    try:
        # --- 3. DATABASE CONNECTION (Docker Optimized) ---
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={DB_SERVER},1433;"
            f"DATABASE={DB_NAME};"
            f"UID={DB_USER};"
            f"PWD={DB_PASS};"
            "Encrypt=yes;"
            "TrustServerCertificate=yes;"
            "ConnectTimeout=30;"
        )
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        print("✅ Successfully connected to SQL Server.")

        # --- 4. EXTRACTION ---
        if not os.path.exists(SOURCE_DIR):
            print(f"Error: Source folder not found at {SOURCE_DIR}")
            return

        files = [f for f in os.listdir(SOURCE_DIR) if f.endswith('.csv')]
        
        if not files:
            print("Empty-handed: No CSV files found.")
            return

        for filename in files:
            file_path = os.path.join(SOURCE_DIR, filename)
            print(f"Extracting & Transforming: {filename}")
            
            # Load data and standardize headers to lowercase
            df = pd.read_csv(file_path)
            df.columns = [c.strip().lower() for c in df.columns]

            # --- 5. TRANSFORMATION (Per Mapping Document) ---
            
            # CUSTOMER_FULL_NAME: Combine First and Last Name and Title Case it
            df['customer_full_name'] = (df['first_name'] + ' ' + df['last_name']).str.title()

            # CALENDAR_DATE: Convert string to Date
            df['calendar_date'] = pd.to_datetime(df['trans_date']).dt.date

            # REPORTING_REGION: Uppercase for reporting consistency
            df['reporting_region'] = df['region'].str.upper()

            # NET_SALES_AMT: Clean numeric value (Removes $ and commas)
            if df['raw_amount'].dtype == 'object':
                df['net_sales_amt'] = pd.to_numeric(df['raw_amount'].str.replace(r'[\$,]', '', regex=True))
            else:
                df['net_sales_amt'] = df['raw_amount']

            # TAX_AMT: Calculated Field (Net * Tax Rate)
            df['tax_amt'] = df['net_sales_amt'] * df['tax_rate']

            # GROSS_SALES_AMT: Total Revenue (Net + Tax)
            df['gross_sales_amt'] = df['net_sales_amt'] + df['tax_amt']

            # LOAD_TIMESTAMP
            df['load_timestamp'] = datetime.now()

            # --- 6. LOADING ---
            for index, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO dbo.FIN_SALES_REPORTING_BASE (
                        TRANS_ID, CUSTOMER_FULL_NAME, CALENDAR_DATE, 
                        REPORTING_REGION, NET_SALES_AMT, TAX_AMT, 
                        GROSS_SALES_AMT, LOAD_TIMESTAMP
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, 
                row['order_id'], 
                row['customer_full_name'], 
                row['calendar_date'],
                row['reporting_region'],
                row['net_sales_amt'],
                row['tax_amt'],
                row['gross_sales_amt'],
                row['load_timestamp']
                )
            
            conn.commit()
            print(f"Successfully loaded {len(df)} rows into Target_Sales_Table.")

            # --- 7. ARCHIVING ---
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