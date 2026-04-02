import sqlite3
import logging
import csv
from datetime import datetime, timedelta

# Configurazione base
DB_PATH = "data/power_bi_metadata.db"
TABLE_NAME = "power_bi_asset_analytics"
CSV_OUTPUT_PATH = "data/output.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def fetch_mock_metadata():
    """
    Simula una risposta della Power BI REST API.
    In una versione reale, questa funzione verrebbe sostituita
    da chiamate API autenticate.
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
    Calcola uno status derivato per il monitoraggio BI.
    TODO: assicurati di saper spiegare questa logica al colloquio.
    """
    if refresh_status == "Failed":
        return "Refresh Failed"

    if last_viewed:
        last_viewed_dt = datetime.fromisoformat(last_viewed)
        if datetime.now() - last_viewed_dt > timedelta(days=30):
            return "Stale"
        return "Active"

    return "Unknown"


def transform_metadata(raw_data):
    """
    Trasforma i metadata grezzi in un dataset analytics-ready.
    """
    transformed = []
    sync_time = datetime.now().isoformat()

    for record in raw_data:
        transformed_record = {
            "asset_id": record["id"],
            "asset_name": record["name"],
            "asset_type": record["type"],
            "owner": record["owner"],
            "workspace_name": record["workspace"],
            "last_updated_at": record["last_updated"],
            "last_viewed_at": record["last_viewed"],
            "views_last_30d": record["views_last_30d"],
            "last_refresh_at": record["last_refresh"],
            "refresh_status": record["refresh_status"],
            "refresh_frequency": record["refresh_frequency"],
            "status": derive_status(
                record["refresh_status"],
                record["last_viewed"]
            ),
            "last_synced_at": sync_time
        }

        transformed.append(transformed_record)

    return transformed


def create_table(conn):
    """
    Crea la tabella di output se non esiste già.
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
    Inserisce o aggiorna i record nello storage SQLite.
    """
    for record in records:
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
        """, record)

    conn.commit()
    logging.info(f"Upsert completed for {len(records)} records")


def export_to_csv(records, path=CSV_OUTPUT_PATH):
    """
    Esporta il dataset finale in CSV per renderlo leggibile su GitHub.
    """
    if not records:
        logging.warning("No records to export")
        return

    fieldnames = records[0].keys()

    with open(path, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    logging.info(f"CSV exported to {path}")


def main():
    """
    Orchestrazione principale:
    1. Recupera metadata
    2. Trasforma i dati
    3. Salva su SQLite
    4. Esporta CSV
    """
    logging.info("Script started")
    conn = None

    try:
        raw_data = fetch_mock_metadata()
        logging.info(f"Fetched {len(raw_data)} records")

        transformed_data = transform_metadata(raw_data)

        conn = sqlite3.connect(DB_PATH)
        create_table(conn)
        upsert_assets(conn, transformed_data)
        export_to_csv(transformed_data)

        logging.info("Data successfully saved")

    except Exception as error:
        logging.error(f"Error during execution: {error}")
        raise

    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed")

    logging.info("Script completed")


if __name__ == "__main__":
    main()