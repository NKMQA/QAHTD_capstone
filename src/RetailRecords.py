import pandas as pd
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os

# ======================================================
# Load environment variables from .env
# ======================================================
load_dotenv()

db_config = {
    "host": os.getenv("MYSQL_HOST"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": os.getenv("MYSQL_DB"),
    "port": int(os.getenv("MYSQL_PORT", 3306))
}

# ======================================================
# Read Excel file
# ======================================================
file_path = "data/training_retail_supply_chain_data.xlsx"
df = pd.read_excel(file_path)
df.columns = [col.strip().replace(" ", "_") for col in df.columns]  # Clean column names

print(f"📄 Loaded {len(df)} records and {len(df.columns)} columns from Excel.")
print("Columns:", df.columns.tolist())

try:
    # ======================================================
    # Connect to MySQL
    # ======================================================
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    print("✅ Connected to MySQL successfully.")

    # ======================================================
    # Create table (matches Excel exactly)
    # ======================================================
    create_table_query = """
    CREATE TABLE IF NOT EXISTS retail_records (
        Date DATE,
        Product_ID VARCHAR(50),
        Product_Name VARCHAR(255),
        Category VARCHAR(100),
        Units_Sold INT,
        Revenue DECIMAL(15,2),
        Stock_Level INT,
        Reorder_Threshold INT,
        Supplier_ID VARCHAR(50),
        Supplier_Rating DECIMAL(5,2),
        Discount_Applied DECIMAL(10,2),
        Return_Flag INT
    );
    """
    cursor.execute(create_table_query)
    print("🗃️ Table 'retail_records' checked/created successfully.")

    # Optional: clear old records (if any)
    cursor.execute("DELETE FROM retail_records;")
    connection.commit()

    # ======================================================
    # Prepare dynamic insert query
    # ======================================================
    columns = [col.strip().replace(" ", "_") for col in df.columns]
    placeholders = ", ".join(["%s"] * len(columns))
    col_names = ", ".join(columns)
    insert_query = f"INSERT INTO retail_records ({col_names}) VALUES ({placeholders})"

    # Insert records
    for _, row in df.iterrows():
        cursor.execute(insert_query, tuple(row))

    connection.commit()
    print(f"✅ {len(df)} records inserted successfully into MySQL.")

    # ======================================================
    # Export table data back to Excel
    # ======================================================
    result_df = pd.read_sql("SELECT * FROM retail_records", connection)
    os.makedirs("outputs", exist_ok=True)
    result_df.to_excel("outputs/targetrecord_output.xlsx", index=False)
    print("📊 Exported MySQL table data to outputs/targetrecord_output.xlsx")

except Error as e:
    print("❌ MySQL Error:", e)

finally:
    if 'connection' in locals() and connection.is_connected():
        cursor.close()
        connection.close()
        print("🔒 MySQL connection closed.")
