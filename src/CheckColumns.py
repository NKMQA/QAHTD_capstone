import pandas as pd

file_path = "data/training_retail_supply_chain_data.xlsx"
df = pd.read_excel(file_path)
print("Total Columns:", len(df.columns))
print("Column Names:")
print(df.columns.tolist())


import pandas as pd

df = pd.read_excel("data/training_retail_supply_chain_data.xlsx")
print(df.columns.tolist())

