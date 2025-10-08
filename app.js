
import pandas as pd
import sqlite3

csv_url = "https://datahub.io/core/gdp/r/gdp.csv"
db_name = "Banks.db"
table_name = "Largest_banks"

# Read CSV
df = pd.read_csv(csv_url)

# Keep latest year per country
df_recent = df.sort_values("Year", ascending=False).drop_duplicates(subset=["Country Name"])

# Convert GDP to billions USD
df_recent["GDP_USD_billions"] = (df_recent["Value"].astype(float) / 1_000_000_000).round(2)

# Keep relevant columns
df_final = df_recent[["Country Name", "GDP_USD_billions"]].copy()
df_final.columns = ["Country", "GDP_USD_billions"]

# Save to SQLite
conn = sqlite3.connect(db_name)
df_final.to_sql(table_name, conn, if_exists="replace", index=False)
conn.close()

print("✅ Database created successfully:", db_name)
print(df_final.head())
