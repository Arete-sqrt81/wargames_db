import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("sqlite:///db/wahapedia.db")

df = pd.read_sql("SELECT * FROM datasheets_abilities;", engine)
# Case-insensitive search for 'lethal hits' in the HTML description
lethal_hits_df = df[df['description'].str.contains('lethal hits', case=False, na=False)]

#print(lethal_hits_df[['datasheet_id', 'name', 'description']])


query_keywords = """
SELECT *
FROM datasheets_keywords
WHERE datasheet_id = '000000460'
"""

df_units = pd.read_sql(query_keywords, engine)
#print(df_units)

query_faction = """
SELECT f.name AS faction_name
FROM datasheets d
JOIN factions f ON d.faction_id = f.id
WHERE d.id = '000000460'
"""
df_faction = pd.read_sql(query_faction, engine)
#print(df_faction)

def print_table_columns(table_name):
    query = f"PRAGMA table_info({table_name})"
    df = pd.read_sql(query, engine)
    print(df)

# Print column names of each table
#tables = ['datasheets_abilities', 'datasheets_keywords', 'datasheets', 'factions', 'datasheets_models']
# for table in tables:
#     print(f"Table: {table}")
#     print_table_columns(table)
#     print()

query = """
SELECT name
FROM sqlite_master
WHERE type='table'
"""
df = pd.read_sql(query, engine)
print(df)