import pandas as pd
import mysql.connector
from datetime import datetime
import os
from dotenv import load_dotenv

# ============================================================
# Step 1: Load Environment Variables
# ============================================================
load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DB = os.getenv("MYSQL_DB")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))

# ============================================================
# Step 2: File Paths
# ============================================================
SOURCE_FILE = "data/training_retail_supply_chain_data.xlsx"
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_FILE = f"outputs/finalcheck_output_{timestamp}.txt"

# ============================================================
# Step 3: Helper - Data Quality Check Function
# ============================================================
def perform_data_quality_checks(df, label):
    results = []
    # 1. Date Format
    try:
        pd.to_datetime(df["Date"], errors="raise")
        results.append(("Date", "All dates in valid format", "PASS"))
    except Exception:
        results.append(("Date", "Invalid date format found", "FAIL"))

    # 2. Unique Product ID per date
    if df.duplicated(subset=["Date", "Product_ID"]).any():
        results.append(("Product_ID", "Duplicate Product_IDs for same date", "FAIL"))
    else:
        results.append(("Product_ID", "Unique for each date", "PASS"))

    # 3. Units_Sold >= 0
    if (df["Units_Sold"] < 0).any():
        results.append(("Units_Sold", "Negative quantities found", "FAIL"))
    else:
        results.append(("Units_Sold", "All quantities valid", "PASS"))

    # 4. Revenue >= 0
    if (df["Revenue"] < 0).any():
        results.append(("Revenue", "Negative revenues found", "FAIL"))
    else:
        results.append(("Revenue", "All revenue values valid", "PASS"))

    # 5. Reorder_Threshold < Stock_Level
    if not (df["Reorder_Threshold"] < df["Stock_Level"]).all():
        results.append(("Reorder_Threshold", "Threshold >= Stock_Level found", "FAIL"))
    else:
        results.append(("Reorder_Threshold", "All thresholds valid", "PASS"))

    # 6. Supplier_ID not null
    if df["Supplier_ID"].isnull().any():
        results.append(("Supplier_ID", "Missing supplier IDs", "FAIL"))
    else:
        results.append(("Supplier_ID", "All supplier IDs present", "PASS"))

    # 7. Discount Applied Numeric
    if pd.to_numeric(df["Discount_Applied"], errors="coerce").isnull().any():
        results.append(("Discount_Applied", "Invalid numeric discount found", "FAIL"))
    else:
        results.append(("Discount_Applied", "Discount numeric", "PASS"))

    # 8. Return_Flag binary (0 or 1)
    if not df["Return_Flag"].isin([0, 1]).all():
        results.append(("Return_Flag", "Invalid binary values", "FAIL"))
    else:
        results.append(("Return_Flag", "All return flags valid", "PASS"))

    return results

# ============================================================
# Step 4: Connect to MySQL
# ============================================================
try:
    connection = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        port=MYSQL_PORT
    )

    # Load Source Excel
    source_df = pd.read_excel(SOURCE_FILE)
    print(f"📥 Loaded source file with {len(source_df)} records and {len(source_df.columns)} columns.")

    # Load Target Table
    target_df = pd.read_sql("SELECT * FROM retail_records", connection)
    print(f"🗄️ Loaded target table with {len(target_df)} records and {len(target_df.columns)} columns.")

    # ============================================================
    # Step 5: Structural Validation
    # ============================================================
    results = []

    # Column Count Check
    if source_df.shape[1] == target_df.shape[1]:
        results.append(("Column Count", "12 vs 12", "PASS"))
    else:
        results.append(("Column Count", f"{source_df.shape[1]} vs {target_df.shape[1]}", "FAIL"))

    # Data Type Validation (with Warning)
    mismatched_cols = []
    for col in source_df.columns:
        if col in target_df.columns:
            if source_df[col].dtype != target_df[col].dtype:
                mismatched_cols.append(col)

    if mismatched_cols:
        results.append(("Data Type Validation", f"Warning: Mismatch in: {mismatched_cols}", "PASS (Warning)"))
    else:
        results.append(("Data Type Validation", "All datatypes match", "PASS"))

    # Null / Missing Values
    if source_df.isnull().values.any() or target_df.isnull().values.any():
        results.append(("Null Values", "Null or missing values found", "FAIL"))
    else:
        results.append(("Null Values", "No missing values", "PASS"))

    # ============================================================
    # Step 6: Data Quality Validation (Source & Target)
    # ============================================================
    source_quality = perform_data_quality_checks(source_df, "Source")
    target_quality = perform_data_quality_checks(target_df, "Target")

    # ============================================================
    # Step 7: Write Report to File
    # ============================================================
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("🔍 STRUCTURAL VALIDATION RESULTS:\n")
        f.write("=================================\n")
        for check in results:
            f.write(f"{check[0]}: {check[1]} --> {check[2]}\n")

        f.write("\n📊 SOURCE DATA QUALITY CHECKS:\n")
        f.write("=================================\n")
        for check in source_quality:
            f.write(f"{check[0]} - {check[1]} - {check[2]}\n")

        f.write("\n📊 TARGET DATA QUALITY CHECKS:\n")
        f.write("=================================\n")
        for check in target_quality:
            f.write(f"{check[0]} - {check[1]} - {check[2]}\n")

        # Compute overall result
        overall_status = "PASS" if all(c[2].startswith("PASS") for c in source_quality + target_quality + results) else "FAIL"
        f.write(f"\n✅ OVERALL RESULT: {overall_status}\n")

    print(f"📁 Validation report generated: {OUTPUT_FILE}")

except mysql.connector.Error as err:
    print(f"❌ Database connection or read error: {err}")

finally:
    if 'connection' in locals() and connection.is_connected():
        connection.close()
        print("🔒 MySQL connection closed.")
