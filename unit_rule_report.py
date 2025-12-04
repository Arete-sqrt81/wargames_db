import pandas as pd
from sqlalchemy import create_engine

DB_PATH = "db/wahapedia.db"
engine = create_engine(f"sqlite:///{DB_PATH}")

# ---- CONFIGURABLE SECTION ----
faction = "tyranids"
keywords = ["lethal hits", "devastating wounds", "anti-infantry"]  # 1a: weapon rules
unit_keywords = ["deep strike", "feel no pain", "invulnerable save", "fights first"]  # 1b: unit rules
# --------------------------------

def build_like_clause(keywords, column):
    return " OR ".join([f"LOWER({column}) LIKE '%{kw.lower()}%'" for kw in keywords])

# 1a: Weapons with any of the keywords
query_weapons = f"""
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
    LOWER(f.name) = '{faction.lower()}'
    AND ({build_like_clause(keywords, 'w.description')})
"""

# 1b: Units with any of the unit_keywords in their abilities
query_abilities = f"""
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
    LOWER(f.name) = '{faction.lower()}'
    AND ({build_like_clause(unit_keywords, 'a.description')})
"""

df_weapons = pd.read_sql(query_weapons, engine)
df_abilities = pd.read_sql(query_abilities, engine)
df_combined = pd.concat([df_weapons, df_abilities], ignore_index=True)

# Show all rows and columns, and set column width to unlimited
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.width', None)

# Print only the most relevant columns for clarity
print(df_combined[["faction_name", "unit_name", "source_type", "source_name", "source_description"]])