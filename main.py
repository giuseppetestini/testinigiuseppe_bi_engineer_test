import sqlite3
import logging
from datetime import datetime, timedelta

# Path del database SQLite locale
DB_PATH = "data/power_bi_metadata.db"

# Nome della tabella che conterrà il dataset analytics-ready
TABLE_NAME = "power_bi_asset_analytics"

# Configurazione base del logging per tracciare l'esecuzione dello script
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def fetch_mock_metadata():
    """
    Simula la risposta di una piattaforma BI API (in questo caso Power BI).
    Restituisce una lista di asset con metadata utili per il monitoraggio BI.
    """
    return [
        {
            "id": "rpt_001",
            "name": "Sales Dashboard",
            "type": "report",
            "owner": "finance_team",
            "workspace": "Finance",
            "last_updated": "2026-03-25T10:00:00",
            "last_viewed": "2026-03-28T09:00:00",
            "views_last_30d": 120,
            "last_refresh": "2026-03-28T08:00:00",
            "refresh_status": "Completed",
            "refresh_frequency": "daily"
        },
        {
            "id": "rpt_002",
            "name": "Marketing Overview",
            "type": "report",
            "owner": "marketing_team",
            "workspace": "Marketing",
            "last_updated": "2026-02-10T10:00:00",
            "last_viewed": "2026-02-15T09:00:00",
            "views_last_30d": 5,
            "last_refresh": "2026-03-20T08:00:00",
            "refresh_status": "Completed",
            "refresh_frequency": "weekly"
        },
        {
            "id": "rpt_003",
            "name": "Operations KPI",
            "type": "dashboard",
            "owner": "ops_team",
            "workspace": "Operations",
            "last_updated": "2026-03-20T10:00:00",
            "last_viewed": "2026-03-29T09:00:00",
            "views_last_30d": 60,
            "last_refresh": "2026-03-29T08:00:00",
            "refresh_status": "Failed",
            "refresh_frequency": "daily"
        }
    ]


def derive_status(refresh_status, last_viewed):
    """
    Calcola uno status derivato utile all'analisi.

    Regole:
    - Se il refresh è fallito -> Refresh Failed
    - Se l'ultima visualizzazione risale a più di 30 giorni fa -> Stale
    - Se il report/dashboard è stato visto recentemente -> Active
    - Se mancano informazioni sufficienti -> Unknown
    """
    if refresh_status == "Failed":
        return "Refresh Failed"

    if last_viewed:
        last_viewed_dt = datetime.fromisoformat(last_viewed)
        if datetime.now() - last_viewed_dt > timedelta(days=30):
            return "Stale"
        else:
            return "Active"

    return "Unknown"


def transform_metadata(raw_data):
    """
    Trasforma i metadata grezzi in un dataset analytics-ready
    con schema coerente e campo derivato 'status'.
    """
    transformed = []

    # Timestamp unico di sincronizzazione per tutto il run corrente
    sync_time = datetime.now().isoformat()

    for r in raw_data:
        status = derive_status(r["refresh_status"], r["last_viewed"])

        record = {
            "asset_id": r["id"],
            "asset_name": r["name"],
            "asset_type": r["type"],
            "owner": r["owner"],
            "workspace_name": r["workspace"],
            "last_updated_at": r["last_updated"],
            "last_viewed_at": r["last_viewed"],
            "views_last_30d": r["views_last_30d"],
            "last_refresh_at": r["last_refresh"],
            "refresh_status": r["refresh_status"],
            "refresh_frequency": r["refresh_frequency"],
            "status": status,
            "last_synced_at": sync_time
        }

        transformed.append(record)

    return transformed


def create_table(conn):
    """
    Crea la tabella SQLite se non esiste già.
    La tabella rappresenta il layer analytics-ready del task.
    """
    query = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        asset_id TEXT PRIMARY KEY,
        asset_name TEXT,
        asset_type TEXT,
        owner TEXT,
        workspace_name TEXT,
        last_updated_at TEXT,
        last_viewed_at TEXT,
        views_last_30d INTEGER,
        last_refresh_at TEXT,
        refresh_status TEXT,
        refresh_frequency TEXT,
        status TEXT,
        last_synced_at TEXT
    )
    """
    conn.execute(query)
    conn.commit()
    logging.info(f"Table '{TABLE_NAME}' created or already exists")


def upsert_assets(conn, records):
    """
    Inserisce o aggiorna i record nella tabella SQLite.
    Usa INSERT OR REPLACE per simulare una logica di upsert semplice.
    """
    for r in records:
        conn.execute(f"""
        INSERT OR REPLACE INTO {TABLE_NAME} VALUES (
            :asset_id,
            :asset_name,
            :asset_type,
            :owner,
            :workspace_name,
            :last_updated_at,
            :last_viewed_at,
            :views_last_30d,
            :last_refresh_at,
            :refresh_status,
            :refresh_frequency,
            :status,
            :last_synced_at
        )
        """, r)

    conn.commit()
    logging.info(f"Upsert completed for {len(records)} records")


def main():
    """
    Orchestrazione principale dello script:
    1. Recupera i metadata
    2. Li trasforma nello schema finale
    3. Crea/verifica la tabella
    4. Salva i dati nel database SQLite
    """
    logging.info("Script started")

    conn = None

    try:
        raw = fetch_mock_metadata()
        logging.info(f"Fetched {len(raw)} records")

        transformed = transform_metadata(raw)

        conn = sqlite3.connect(DB_PATH)
        create_table(conn)
        upsert_assets(conn, transformed)

        logging.info("Data successfully saved")

    except Exception as e:
        logging.error(f"Error during execution: {e}")
        raise

    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed")

    logging.info("Script completed")


if __name__ == "__main__":
    main()

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute(f"SELECT * FROM {TABLE_NAME}")
rows = cursor.fetchall()

for row in rows:
    print(row)

conn.close()