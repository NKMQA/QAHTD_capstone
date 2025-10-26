# 🧠 ETL Testing & Validation Project (Retail Supply Chain Data)

## 📘 Overview
This project demonstrates ETL Testing using Python and MySQL.  
It validates data quality in a source Excel file, loads it into a MySQL database, and performs source-to-target validation to ensure accuracy and consistency.

---

## 🧩 Project Workflow

| Step | Script | Description | Output |
|------|---------|--------------|----------|
| 1️⃣ | `InitialCheck.py` | Validates source Excel file for data quality issues. | `outputs/initialcheck_output_<timestamp>.txt` |
| 2️⃣ | `RetailRecords.py` | Loads data from Excel → MySQL and exports target snapshot. | `outputs/targetrecord_output.xlsx` |
| 3️⃣ | `Source_vs_Target.py` | Performs structural & data validation between source and MySQL target. | `outputs/finalcheck_output_<timestamp>.txt` |

---

## 🗄️ Source & Target Details

| Layer | Name | Type | Description |
|--------|------|------|-------------|
| **Source** | `training_retail_supply_chain_data.xlsx` | Excel | Original input dataset |
| **Target** | `retail_records` | MySQL Table | Loaded ETL output in database |
| **Target Snapshot** | `targetrecord_output.xlsx` | Excel | Copy of target table (for validation) |

---

## ⚙️ Tech Stack
- **Python Libraries:** pandas, openpyxl, mysql-connector-python, python-dotenv  
- **Database:** MySQL Workbench  
- **IDE:** Visual Studio Code  
- **(Optional)**: Azure Data Factory, Databricks  

---

## 🧠 Data Quality Rules
- All dates must be valid  
- Product IDs unique per date  
- Units_Sold and Revenue ≥ 0  
- Reorder_Threshold < Stock_Level  
- Supplier_ID not null  
- Discount numeric  
- Return_Flag binary (0 or 1)

---

## 🧾 Summary
This project performs:
- Data validation at source and target levels  
- End-to-end ETL loading (Excel → MySQL)  
- Structural and data consistency verification  
- Automated report generation for test evidence
