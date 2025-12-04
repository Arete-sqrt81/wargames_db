import pandas as pd
from sqlalchemy import create_engine

DB_PATH = "db/wahapedia.db"
engine = create_engine(f"sqlite:///{DB_PATH}")

query_weapons = """
SELECT DISTINCT
    f.name AS faction_name,
    d.name AS unit_name,
    w.name AS source_name,
    'weapon' AS source_type,
    w.description AS source_description
FROM datasheets_wargear w
JOIN datasheets d ON w.datasheet_id = d.id
JOIN factions f ON d.faction_id = f.id
WHERE
    LOWER(f.name) = 'tyranids'
    AND LOWER(w.description) LIKE '%lethal hits%'
"""

query_abilities = """
SELECT DISTINCT
    f.name AS faction_name,
    d.name AS unit_name,
    a.name AS source_name,
    'ability' AS source_type,
    a.description AS source_description
FROM datasheets_abilities a
JOIN datasheets d ON a.datasheet_id = d.id
JOIN factions f ON d.faction_id = f.id
WHERE
    LOWER(f.name) = 'tyranids'
    AND LOWER(a.description) LIKE '%lethal hits%'
"""

df_weapons = pd.read_sql(query_weapons, engine)
df_abilities = pd.read_sql(query_abilities, engine)
df_combined = pd.concat([df_weapons, df_abilities], ignore_index=True)
print(df_combined)