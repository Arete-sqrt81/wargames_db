import pandas as pd
from sqlalchemy import create_engine

DB_PATH = "db/wahapedia.db"
engine = create_engine(f"sqlite:///{DB_PATH}")

import re

def parse_save(sv):
    """Parse '3+' as 3, '2+' as 2, etc."""
    if isinstance(sv, str) and sv.endswith('+'):
        return int(sv[:-1])
    return None

def extract_num_models(description):
    """Extract the number of models from cost_description, e.g. '5 models' -> 5"""
    match = re.match(r"(\d+)", str(description))
    if match:
        return int(match.group(1))
    return 1

# Query for unit chassis stats (no character filtering yet)
query_units = """
SELECT DISTINCT
    m.name AS model_name,
    m.W
FROM datasheets_models m
LEFT JOIN datasheets_keywords k ON m.datasheet_id = k.datasheet_id AND k.keyword = 'Character'
WHERE m.W = 4 OR m.W = 5
AND k.datasheet_id IS NULL
"""

df_units = pd.read_sql(query_units, engine)
#df_units['parsed_sv'] = df_units['Sv'].apply(parse_save)
result = df_units
print(result)

# # "Terminator" filter: Sv 2+ AND max 5 W
# filtered_units = df_units[(df_units['parsed_sv'] == 2) & (df_units['W'].astype(int) <= 5)].copy()

# # Calculate number of models, points per model, and points per wound
# filtered_units['num_models'] = filtered_units['cost_description'].apply(extract_num_models)
# filtered_units['cost'] = pd.to_numeric(filtered_units['cost'], errors='coerce')
# filtered_units['W'] = pd.to_numeric(filtered_units['W'], errors='coerce')

# filtered_units['points_per_model'] = filtered_units['cost'] / filtered_units['num_models']
# filtered_units['points_per_wound'] = filtered_units['cost'] / (filtered_units['num_models'] * filtered_units['W'])

# # Round for display
# filtered_units['points_per_model'] = filtered_units['points_per_model'].round(1)
# filtered_units['points_per_wound'] = filtered_units['points_per_wound'].round(2)

# filtered_units = filtered_units.copy()

# # Select columns of interest
# result = filtered_units[['unit_name', 'Sv', 'cost', 'cost_description', 'num_models', 'points_per_model', 'W', 'points_per_wound']]

# # Sort by points_per_wound descending
# result = result.sort_values(by="points_per_wound", ascending=False)
# print(result)

# Example: Query for weapon stats (kept for future use)
query_weapons = """
SELECT
    f.name AS faction_name,
    d.name AS unit_name,
    w.name AS weapon_name,
    w.range, w.A, w.BS_WS, w.S, w.AP, w.D
FROM datasheets_wargear w
JOIN datasheets d ON w.datasheet_id = d.id
JOIN factions f ON d.faction_id = f.id
WHERE LOWER(f.name) = 'space marines'
"""

# Uncomment below if/when you want to use weapon data:
# df_weapons = pd.read_sql(query_weapons, engine)
# print(df_weapons.head())



query_furies = """
SELECT *
FROM datasheets_models m
JOIN datasheets d ON m.datasheet_id = d.id
JOIN factions f ON d.faction_id = f.id
WHERE f.name = 'Chaos Daemons'
AND d.name = 'Furies'
"""

df_furies = pd.read_sql(query_furies, engine)
#print(df_furies)