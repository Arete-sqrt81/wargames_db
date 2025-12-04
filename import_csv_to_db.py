import os
import pandas as pd
from sqlalchemy import create_engine

CSV_FOLDER = "waha_csv"
DB_FOLDER = "db"
DB_PATH = os.path.join(DB_FOLDER, "wahapedia.db")

def load_csv_to_sql(csv_path, table_name, engine):
    df = pd.read_csv(
        csv_path,
        delimiter='|',
        dtype=str,
        encoding='utf-8'
    )
    df = df.loc[:, ~df.columns.str.match('^Unnamed')]
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Loaded {csv_path} into {table_name} table.")

if __name__ == "__main__":
    # Ensure db folder exists
    os.makedirs(DB_FOLDER, exist_ok=True)
    engine = create_engine(f"sqlite:///{DB_PATH}")

    for filename in os.listdir(CSV_FOLDER):
        if filename.lower().endswith(".csv"):
            table_name = os.path.splitext(filename)[0].lower()
            csv_path = os.path.join(CSV_FOLDER, filename)
            load_csv_to_sql(csv_path, table_name, engine)