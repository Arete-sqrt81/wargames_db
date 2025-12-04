import sqlite3
import pandas as pd
import os

# Config
STAR_DB_PATH = os.path.join('processed', 'warhammer_star.db')

# Connect to star DB
conn = sqlite3.connect(STAR_DB_PATH)
cursor = conn.cursor()

# Create view: Joins fact_units with dims for easy stats
cursor.execute('''
CREATE VIEW IF NOT EXISTS v_unit_stats AS
SELECT 
    fu.unit_id,
    fu.model_name,
    fu.unit_name,
    df.faction_name,
    dr.role_name,
    fu.movement,
    fu.toughness,
    fu.save,
    fu.invulnerable_save,
    fu.wounds,
    fu.leadership,
    fu.objective_control
FROM fact_units fu
JOIN dim_factions df ON fu.faction_id = df.faction_id
JOIN dim_roles dr ON fu.role_id = dr.role_id
WHERE fu.toughness IS NOT NULL AND fu.wounds IS NOT NULL;
''')

# Commit
conn.commit()

# Test query with Pandas
df = pd.read_sql_query("""
SELECT 
    faction_name, 
    role_name, 
    AVG(toughness) as avg_toughness, 
    AVG(wounds) as avg_wounds 
FROM v_unit_stats 
WHERE faction_name = 'Space Marines' 
GROUP BY faction_name, role_name
LIMIT 10
""", conn)
print("Sample query results:")
print(df)

# Close
conn.close()
print(f"View v_unit_stats created in {STAR_DB_PATH}. Test query executed.")