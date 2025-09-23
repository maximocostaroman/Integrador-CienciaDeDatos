import os
import pandas as pd

# Directorios dentro del repo
BASE = "include/data/processed"
IN_FILE = os.path.join(BASE, "warehouse", "flights_min_daily_all.csv")
OUT_DIR = os.path.join(BASE, "warehouse")
os.makedirs(OUT_DIR, exist_ok=True)

if not os.path.exists(IN_FILE):
    raise SystemExit("No encontré el archivo consolidado de vuelos: " + IN_FILE)

# Leer dataset consolidado
df = pd.read_csv(IN_FILE)

print(f" Dataset de vuelos leído: {len(df)} filas")

# Extraer destinos únicos con metadata
routes = (
    df[["snapshot_id", "snapshot_date", "destination_city"]]
    .dropna()
    .drop_duplicates()
    .sort_values(["snapshot_date", "destination_city"])
)

# Dataset reducido solo de destinos distintos
unique_dests = (
    df[["destination_city"]]
    .dropna()
    .drop_duplicates()
    .sort_values("destination_city")
    .reset_index(drop=True)
)

# Guardar ambos: lista histórica y lista única
out_hist = os.path.join(OUT_DIR, "routes_history.csv")
out_unique = os.path.join(OUT_DIR, "routes_unique.csv")

routes.to_csv(out_hist, index=False)
unique_dests.to_csv(out_unique, index=False)

print(f" Guardado histórico: {out_hist} ({len(routes)} filas)")
print(f" Guardado únicos: {out_unique} ({len(unique_dests)} destinos)")
