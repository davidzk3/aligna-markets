import psycopg
from pathlib import Path

dsn = "postgresql://pmops:rwSBZJh28VhNCAIm05mKIzLZSYwL5Z6A@dpg-d7fvqb17vvec73djhqn0-a.oregon-postgres.render.com/pmops?sslmode=require"

files = [
    "apps/api/sql/005_market_uma_resolution_intelligence.sql",
    "apps/api/sql/006_market_oracle_link_candidates.sql",
    "apps/api/sql/007_market_resolved_learning_daily.sql",
    "apps/api/sql/008_market_resolution_patterns.sql",
    "apps/api/sql/009_market_design_intelligence.sql",
    "apps/api/sql/010_market_design_rewrite_intelligence.sql",
]

conn = psycopg.connect(dsn)
conn.autocommit = True
cur = conn.cursor()

for f in files:
    print(f"Running {f}")
    sql = Path(f).read_text(encoding="utf-8")
    cur.execute(sql)

cur.close()
conn.close()

print("DONE")