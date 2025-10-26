import pandas as pd
import os
from datetime import datetime

# Paths
BASE = os.path.dirname(os.path.dirname(__file__))
DATA_PATH = os.path.join(BASE, "data", "training_retail_supply_chain_data.xlsx")
OUT_DIR = os.path.join(BASE, "outputs")
os.makedirs(OUT_DIR, exist_ok=True)
ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
OUT_FILE = os.path.join(OUT_DIR, f"initialcheck_output_{ts}.txt")

df = pd.read_excel(DATA_PATH, engine='openpyxl')

results = []
overall_pass = True

def check(condition, success_msg, fail_msg):
    global overall_pass
    if condition:
        results.append(f"PASS: {success_msg}")
    else:
        results.append(f"FAIL: {fail_msg}")
        overall_pass = False

# 1. Date format
try:
    pd.to_datetime(df["Date"], errors="raise")
    check(True, "Date - All dates valid", "")
except Exception:
    check(False, "", "Date - Invalid date format found")

# 2. Unique Product_ID per Date
dupes = df[df.duplicated(subset=["Date", "Product_ID"], keep=False)]
check(dupes.empty, "Unique Product ID - No duplicates for same date",
      f"Duplicate found rows: {list(dupes.index + 2)}")

# 3. Units_Sold >= 0
neg_units = df[df["Units_Sold"] < 0]
check(neg_units.empty, "Units_Sold >= 0", f"Negative found rows: {list(neg_units.index + 2)}")

# 4. Revenue >= 0
neg_revenue = df[df["Revenue"] < 0]
check(neg_revenue.empty, "Revenue >= 0", f"Negative found rows: {list(neg_revenue.index + 2)}")

# 5. Reorder_Threshold < Stock_Level
bad_threshold = df[df["Reorder_Threshold"] >= df["Stock_Level"]]
check(bad_threshold.empty, "Reorder_Threshold < Stock_Level",
      f"Threshold >= Stock rows: {list(bad_threshold.index + 2)}")

# 6. Supplier_ID not null
null_suppliers = df[df["Supplier_ID"].isna()]
check(null_suppliers.empty, "Supplier_ID not null", f"Missing Supplier_ID rows: {list(null_suppliers.index + 2)}")

# 7. Discount_Applied numeric
non_numeric = pd.to_numeric(df["Discount_Applied"], errors='coerce').isna()
check(non_numeric.sum() == 0, "Discount_Applied is numeric",
      f"Non-numeric discount rows: {list(df[non_numeric].index + 2)}")

# 8. Return_Flag binary (0 or 1)
invalid_return = df[~df["Return_Flag"].isin([0,1])]
check(invalid_return.empty, "Return_Flag is binary (0/1)",
      f"Invalid return flags rows: {list(invalid_return.index + 2)}")

# Write results
with open(OUT_FILE, "w") as f:
    f.write("Initial Data Quality Check Report\n")
    f.write(f"Timestamp: {ts}\n\n")
    for r in results:
        f.write(r + "\n")
    f.write("\nOVERALL RESULT: " + ("PASS" if overall_pass else "FAIL"))

print(f"✅ Initial check complete. Report saved to {OUT_FILE}")
