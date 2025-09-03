# dags/bue_flights_v3.py
from __future__ import annotations
import os, json, time, math, gzip, shutil
from datetime import timedelta
import pendulum
import requests
import pandas as pd
from airflow.decorators import dag, task



default_args = {
    'owner': 'M Costa y J Lorenzo',
}

# ----------------------------
# Configuración (variables de entorno con defaults)
# ----------------------------
TP_TOKEN      = os.getenv("TP_TOKEN", "")         # <- Debe venir en .env
ORIGIN        = os.getenv("ORIGIN", "BUE")
CURRENCY      = os.getenv("CURRENCY", "usd")
ONE_WAY       = os.getenv("ONE_WAY", "true")      # "true"/"false"
MONTHS_AHEAD  = int(os.getenv("MONTHS_AHEAD", "12"))
MAX_DESTS     = int(os.getenv("MAX_DESTS", "10")) # demo-friendly; 0 = sin tope
DATA_DIR      = os.getenv("DATA_DIR", "/usr/local/airflow/include/data")
TZ_AR         = pendulum.timezone("America/Argentina/Mendoza")

# Retención (opcional)
ENABLE_CLEANUP          = os.getenv("ENABLE_CLEANUP", "false").lower() == "true"
CLEANUP_COMPRESS_DAYS   = int(os.getenv("CLEANUP_COMPRESS_DAYS", "7"))
CLEANUP_DELETE_DAYS     = int(os.getenv("CLEANUP_DELETE_DAYS", "30"))

BASE_V3 = "https://api.travelpayouts.com/aviasales/v3"

# ----------------------------
# Helpers
# ----------------------------
def http_get(url: str, params: dict, max_retries: int = 5) -> dict:
    """GET con backoff para rate limit/5xx."""
    wait = 1.0
    for _ in range(max_retries):
        r = requests.get(url, params=params, timeout=30)
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(wait)
            wait = min(wait * 2, 30)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"GET falló tras {max_retries} reintentos: {url}")

def months_from_now(n: int, anchor: pendulum.DateTime) -> list[str]:
    """['YYYY-MM', ...] desde el mes de anchor hasta +n."""
    y, m = anchor.year, anchor.month
    out = []
    for k in range(n + 1):
        mm = m + k
        y2 = y + (mm - 1) // 12
        m2 = (mm - 1) % 12 + 1
        out.append(f"{y2:04d}-{m2:02d}")
    return out

def tod_label_from_local(dt_local: pendulum.DateTime) -> str:
    h = dt_local.hour
    if   6 <= h < 12: return "morning"
    elif 12 <= h < 19: return "afternoon"
    elif 19 <= h <= 23: return "evening"
    return "night"

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def compress_old_jsons(base_raw_dir: str, compress_after_days: int, delete_after_days: int) -> dict:
    """Comprime .json >N días a .json.gz y borra snapshots >M días."""
    now = pendulum.now("UTC")
    changed, deleted = 0, 0
    if not os.path.isdir(base_raw_dir):
        return {"compressed": changed, "deleted": deleted}
    for snap in os.listdir(base_raw_dir):
        snap_path = os.path.join(base_raw_dir, snap)
        if not os.path.isdir(snap_path): 
            continue
        mtime = pendulum.from_timestamp(os.path.getmtime(snap_path), tz="UTC")
        age_days = (now - mtime).in_days()
        if age_days > delete_after_days:
            shutil.rmtree(snap_path, ignore_errors=True)
            deleted += 1
            continue
        if age_days > compress_after_days:
            for root, _, files in os.walk(snap_path):
                for f in files:
                    if f.endswith(".json") and not os.path.exists(os.path.join(root, f + ".gz")):
                        src = os.path.join(root, f)
                        dst = src + ".gz"
                        with open(src, "rb") as fin, gzip.open(dst, "wb") as fout:
                            shutil.copyfileobj(fin, fout)
                        os.remove(src)
                        changed += 1
    return {"compressed": changed, "deleted": deleted}

# ----------------------------
# DAG
# ----------------------------
@dag(
    dag_id="bue_flights_v3",
    schedule=None,  # Demo: trigger manual. Luego podés usar "0 8,20 * * *"
    start_date=pendulum.datetime(2025, 9, 1, tz=TZ_AR),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["bue","travelpayouts","v3","calendar"]
)
def bue_flights_v3():

    @task
    def snapshot_meta() -> dict:
        """Crea carpeta del snapshot y guarda metadatos: hora local, tod_label, etc."""
        if not TP_TOKEN:
            raise ValueError("Falta TP_TOKEN (.env)")

        now_utc   = pendulum.now("UTC")
        now_local = now_utc.in_timezone(TZ_AR)
        snapshot_id = now_utc.to_iso8601_string()

        meta = {
            "snapshot_id": snapshot_id,
            "snapshot_utc": now_utc.to_iso8601_string(),
            "snapshot_local": now_local.to_iso8601_string(),
            "tod_label": tod_label_from_local(now_local),
            "weekday_search": now_local.format("dddd"),
            "origin": ORIGIN,
            "currency": CURRENCY,
            "one_way": ONE_WAY,
        }
        base = f"{DATA_DIR}/raw/{snapshot_id}"
        ensure_dir(base + "/discover")
        ensure_dir(base + "/calendar")
        with open(f"{base}/snapshot_meta.json", "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
        return meta

    @task
    def discover_routes(meta: dict) -> list[str]:
        """Trae TODOS los destinos (paginado) desde BUE para el mes actual."""
        snapshot_local = pendulum.parse(meta["snapshot_local"])
        beginning = snapshot_local.start_of("month").to_date_string()  # YYYY-MM-DD

        url = f"{BASE_V3}/get_latest_prices"
        page = 1
        by_dest_min = {}  # destino -> mejor precio visto
        total_rows = 0

        while True:
            params = {
                "origin": ORIGIN,
                "group_by": "directions",
                "period_type": "month",
                "beginning_of_period": beginning,
                "one_way": ONE_WAY,
                "currency": CURRENCY,
                "page": page,
                "token": TP_TOKEN,
            }
            js = http_get(url, params)
            rows = js.get("data", []) or []
            if not rows:
                break
            total_rows += len(rows)

            for r in rows:
                dest = r.get("destination")
                if not dest:
                    continue
                val = r.get("value")
                if dest not in by_dest_min or (isinstance(val, (int,float)) and val < by_dest_min[dest]):
                    by_dest_min[dest] = float(val) if isinstance(val, (int,float)) else math.inf

            page += 1
            # pequeña pausa amable (opcional)
            time.sleep(0.02)

        dests_sorted = [d for d,_ in sorted(by_dest_min.items(), key=lambda kv: kv[1])]
        if MAX_DESTS > 0:
            dests_sorted = dests_sorted[:MAX_DESTS]

        out_csv = f"{DATA_DIR}/raw/{meta['snapshot_id']}/discover/dests.csv"
        pd.DataFrame({"destination": dests_sorted}).to_csv(out_csv, index=False)

        return dests_sorted

    @task
    def build_months(meta: dict) -> list[str]:
        """Mes actual local + 12 siguientes, en formato YYYY-MM."""
        snap_local = pendulum.parse(meta["snapshot_local"])
        return months_from_now(MONTHS_AHEAD, snap_local)

    @task
    def fetch_calendar_for_dest(dest: str, months: list[str], meta: dict) -> str:
        """
        Pide /grouped_prices para TODOS los meses de un destino y guarda
        UN solo JSON compacto por destino (reduce cantidad de archivos).
        """
        url = f"{BASE_V3}/grouped_prices"
        months_blob = {} 

        for mm in months:
            params = {
                "origin": ORIGIN,
                "destination": dest,
                "departure_at": mm,           # YYYY-MM
                "group_by": "departure_at",
                "one_way": ONE_WAY,
                "currency": CURRENCY,
                "token": TP_TOKEN,
            }
            js = http_get(url, params)         # GET con backoff
            months_blob[mm] = js 
            time.sleep(0.05)  # amable con rate limit

        out_dir = f"{DATA_DIR}/raw/{meta['snapshot_id']}/calendar"
        ensure_dir(out_dir)
        out_file = f"{out_dir}/{dest}.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump({"destination": dest, "months": months_blob}, f, ensure_ascii=False)  # 1 JSON por destino con todos los meses.
        return out_file

    @task
    def normalize_and_save(meta: dict, dest_files: list[str]) -> str:
        """Convierte los JSON compactos a una tabla plana y la guarda (Parquet/CSV)."""
        rows = []   # diccionario con una fila por (destino, dia de salida)
        snap_local = pendulum.parse(meta["snapshot_local"])

        #Iterar por cada destino (1 JSON por destino)
        for path in dest_files:
            if not path or not os.path.exists(path):
                continue
            with open(path, "r", encoding="utf-8") as f:
                blob = json.load(f)         #JSON por destino con todos los meses
            dest = blob.get("destination")
            months_map = blob.get("months", {})
        
        # Iterar por cada mes y cada día
            for mm, payload in months_map.items():
                data = (payload or {}).get("data", {})
                if not isinstance(data, dict):
                    continue
                # data: {"YYYY-MM-DD": { price, transfers, duration, departure_at, origin_airport, destination_airport, ...}}
                
                # parsea la fecha de la clave a depart_dt (objeto pendulum).
                for day, rec in data.items():
                    try:
                        depart_dt = pendulum.parse(day)
                    except Exception:
                        continue

                    price     = rec.get("price")
                    duration  = rec.get("duration") # en minutos
                    transfers = rec.get("transfers")
                    hours_tot = round((duration or 0) / 60, 1) if duration is not None else None # convertir a horas
                    is_extreme = (transfers is not None and transfers >= 3) or (hours_tot is not None and hours_tot > 40) # flags para marcar extremos

                    month = depart_dt.month
                    season = ("Summer" if month in (12,1,2)         #estacion en el hemisferio sur
                              else "Autumn" if month in (3,4,5)
                              else "Winter" if month in (6,7,8)
                              else "Spring") 

                    rows.append({
                        # Metadatos del snapshot
                        "snapshot_id": meta["snapshot_id"],
                        "snapshot_local": meta["snapshot_local"],
                        "tod_label": meta["tod_label"],
                        "weekday_search": meta["weekday_search"],

                        # Datos de ruta
                        "origin_city": ORIGIN,
                        "origin_airport": rec.get("origin_airport"),
                        "destination_city": dest,
                        "destination_airport": rec.get("destination_airport"),

                        # Fechas
                        "depart_date": depart_dt.to_date_string(),  # solo fecha de salida
                        "departure_at": rec.get("departure_at"),   # fecha y hora de salida (string)
                        
                        #Target y atributos
                        "price_min_raw": price,
                        "transfers": transfers, #escalas
                        "duration_min": duration,
                        "hours_total": hours_tot,
                        "is_extreme": bool(is_extreme),

                        #Calendario
                        "dow": depart_dt.day_name(),    #dia de la semanda de la salida
                        "month": month,
                        "season": season,
                        "days_to_departure": (depart_dt.date() - snap_local.date()).days, #dias faltantes desde el snapshot hasta la fecha de salida.
                    })

        df = pd.DataFrame(rows)
        ensure_dir(f"{DATA_DIR}/processed/flights_min_daily")

        safe_snap = meta["snapshot_id"].replace(":", "-")
        out_parquet = f"{DATA_DIR}/processed/flights_min_daily/flights_min_daily_{safe_snap}.parquet"
        out_csv     = f"{DATA_DIR}/processed/flights_min_daily/flights_min_daily_{safe_snap}.csv"

        try:
            df.to_parquet(out_parquet, index=False)
            return out_parquet
        except Exception:
            df.to_csv(out_csv, index=False)
            return out_csv

    @task
    def cleanup_raw() -> dict:
        """(Opcional) Comprime/borrra RAW antiguo para controlar volumen."""
        if not ENABLE_CLEANUP:
            return {"compressed": 0, "deleted": 0, "skipped": True}
        base_raw = f"{DATA_DIR}/raw"
        res = compress_old_jsons(base_raw, CLEANUP_COMPRESS_DAYS, CLEANUP_DELETE_DAYS)
        res["skipped"] = False
        return res

    # ---------- Orquestación ----------
    meta   = snapshot_meta()
    dests  = discover_routes(meta)
    months = build_months(meta)

    # 1 task por destino: cada task pide TODOS los meses y guarda 1 JSON por destino (compacto)
    dest_files = fetch_calendar_for_dest.partial(months=months, meta=meta).expand(dest=dests)

    normalize_and_save(meta, dest_files) >> cleanup_raw()

dag = bue_flights_v3()
