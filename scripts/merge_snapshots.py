import os, glob
import pandas as pd

# Directorios dentro del repo
BASE = "include/data/processed"
IN_DIR = os.path.join(BASE, "flights_min_daily")
OUT_DIR = os.path.join(BASE, "warehouse")
os.makedirs(OUT_DIR, exist_ok=True)

# Buscar todos los snapshots CSV
files = glob.glob(os.path.join(IN_DIR, "flights_min_daily_*.csv"))

if not files:
    raise SystemExit("⚠️ No encontré snapshots en " + IN_DIR)

print(f"📂 Encontrados {len(files)} snapshots")
dfs = []
for f in sorted(files):
    try:
        df = pd.read_csv(f)
        dfs.append(df)
        print("   OK:", f, "→", len(df), "filas")
    except Exception as e:
        print("   ERROR leyendo", f, ":", e)

df_all = pd.concat(dfs, ignore_index=True)

# Agregar snapshot_date a partir de snapshot_local
df_all["snapshot_date"] = pd.to_datetime(df_all["snapshot_local"]).dt.date

# Deduplicar exactos (opcional, evita filas repetidas si suben el mismo snapshot dos veces)
df_all.drop_duplicates(
    subset=["snapshot_id", "destination_city", "depart_date"],
    keep="last",
    inplace=True,
)

# Guardar el Gold en CSV
out_csv = os.path.join(OUT_DIR, "flights_min_daily_all.csv")
df_all.to_csv(out_csv, index=False)

print("✅ Guardado:", out_csv, "filas:", len(df_all))
