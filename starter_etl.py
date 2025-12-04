import sqlite3
import pandas as pd
import re

# Config: Update RAW_DB_PATH to your raw DB file (e.g., 'waha.db')
RAW_DB_PATH = 'db/wahapedia.db'
STAR_DB_PATH = 'processed/warhammer_star.db'

def parse_stat(stat_text):
    """Parse stats like '3+' → 3, '6"' → 6, '2(5)' → 2."""
    if pd.isna(stat_text) or not str(stat_text).strip():
        return None
    stat_text = str(stat_text).strip()
    if '+' in stat_text:
        return int(re.sub(r'[^0-9]', '', stat_text))
    if '"' in stat_text:
        return int(re.sub(r'[^0-9]', '', stat_text))
    if '(' in stat_text:
        return int(stat_text.split('(')[0])
    try:
        return int(stat_text)
    except ValueError:
        return None

# Connect to raw and star DBs
raw_conn = sqlite3.connect(RAW_DB_PATH)
star_conn = sqlite3.connect(STAR_DB_PATH)

# 1. dim_factions: Direct from factions
factions_df = pd.read_sql_query("SELECT id as faction_id, name as faction_name, link FROM factions", raw_conn)
factions_df.to_sql('dim_factions', star_conn, if_exists='replace', index=False)

# 2. dim_roles: Unique from datasheets.role
roles_df = pd.read_sql_query("SELECT DISTINCT role as role_name FROM datasheets WHERE role IS NOT NULL AND role != ''", raw_conn)
roles_df['role_id'] = range(1, len(roles_df) + 1)
roles_df.to_sql('dim_roles', star_conn, if_exists='replace', index=False)

# 3. dim_keywords: Unique from datasheets_keywords, exclude unit names, case-insensitive categories
keywords_df = pd.read_sql_query("""
    SELECT DISTINCT dk.keyword as keyword_name,
        CASE 
            WHEN dk.is_faction_keyword = '1' OR dk.is_faction_keyword = 1 THEN 'Faction'
            WHEN UPPER(dk.keyword) IN ('INFANTRY', 'VE onsite python courseHICLE', 'CAVALRY', 'MONSTER', 'WALKER', 
                                      'FLY', 'BIKE', 'BEAST', 'SWARM', 'TRANSPORT', 'FORTIFICATION', 
                                      'TITANIC', 'TOWERING', 'MOUNTED', 'BATTLELINE') THEN 'Unit Type'
            WHEN UPPER(dk.keyword) IN ('GRENADES', 'SMOKE', 'LEADER', 'PSYKER') THEN 'Ability'
            ELSE 'Other'
        END as category
    FROM datasheets_keywords dk
    INNER JOIN datasheets d ON dk.datasheet_id = d.id
    WHERE dk.keyword IS NOT NULL 
        AND dk.keyword != '' 
        AND LOWER(TRIM(d.name)) NOT LIKE '%' || LOWER(TRIM(dk.keyword)) || '%'  -- Exclude keywords in unit name
""", raw_conn)
keywords_df['keyword_id'] = range(1, len(keywords_df) + 1)
keywords_df.to_sql('dim_keywords', star_conn, if_exists='replace', index=False)

# 4. fact_units: Join datasheets_models + datasheets + factions, parse stats
fact_df = pd.read_sql_query("""
    SELECT 
        dm.datasheet_id as unit_id,
        dm.line as model_line,
        dm.name as model_name,
        d.name as unit_name,
        d.faction_id,
        d.role,
        dm.M, dm.T, dm.Sv, dm.inv_sv, dm.W, dm.Ld, dm.OC,
        dm.base_size
    FROM datasheets_models dm
    JOIN datasheets d ON dm.datasheet_id = d.id
    WHERE dm.line IS NOT NULL
""", raw_conn)

# Add FKs: Merge for faction_name (for ref), role_id
fact_df = fact_df.merge(
    factions_df[['faction_id', 'faction_name']], 
    on='faction_id', how='left'
)
fact_df = fact_df.merge(
    roles_df[['role_name', 'role_id']], 
    left_on='role', right_on='role_name', how='left'
)

# Parse metrics
fact_df['movement'] = fact_df['M'].apply(parse_stat)
fact_df['toughness'] = fact_df['T'].apply(parse_stat)
fact_df['save'] = fact_df['Sv'].apply(parse_stat)
fact_df['invulnerable_save'] = fact_df['inv_sv'].apply(parse_stat)
fact_df['wounds'] = fact_df['W'].apply(parse_stat)
fact_df['leadership'] = fact_df['Ld'].apply(parse_stat)
fact_df['objective_control'] = fact_df['OC'].apply(parse_stat)

# Select core fact columns
fact_cols = [
    'unit_id', 'model_line', 'model_name', 'faction_id', 'role_id', 
    'movement', 'toughness', 'save', 'invulnerable_save', 'wounds', 
    'leadership', 'objective_control', 'base_size', 'unit_name'
]
fact_df = fact_df[fact_cols]
fact_df.to_sql('fact_units', star_conn, if_exists='replace', index=False)

# Add simple indexes
star_conn.execute("CREATE INDEX IF NOT EXISTS idx_fact_units_faction ON fact_units(faction_id);")
star_conn.execute("CREATE INDEX IF NOT EXISTS idx_fact_units_role ON fact_units(role_id);")

# Close
raw_conn.close()
star_conn.close()

# Verify
print("Starter ETL complete! Tables created in warhammer_star.db")
star_conn = sqlite3.connect(STAR_DB_PATH)
print("Table counts:")
for table in ['dim_factions', 'dim_roles', 'dim_keywords', 'fact_units']:
    count = pd.read_sql_query(f"SELECT COUNT(*) as cnt FROM {table}", star_conn).iloc[0]['cnt']
    print(f"{table}: {count} rows")
star_conn.close()