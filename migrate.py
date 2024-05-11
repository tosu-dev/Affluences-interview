import pandas as pd
from database import engine

# Migrate csv datas to mysql
df = pd.read_csv("dataset.csv")
df.rename(columns={"Unnamed: 0": "id"}, inplace=True)  # Rename for the id column
df.to_sql("history", engine, if_exists='append', index=False)

print("===== Migration done =====")
