# BI Engineer Technical Assignment

## Panoramica
Questo progetto simula una pipeline leggera di ingestione dei metadati di Power BI.

L’obiettivo è costruire un dataset analytics-ready utile per monitorare:
- utilizzo di dashboard e report
- asset non utilizzati (stale)
- errori nei refresh
- stato generale degli asset BI

Per semplicità, i dati sono simulati (mock), ma la struttura riflette un caso reale.

---

## Scelta della piattaforma
Ho scelto **Power BI** perché è lo strumento con cui ho più esperienza lavorativa.

Questo mi ha permesso di concentrarmi su:
- modellazione dei dati
- logiche di monitoraggio BI
- progettazione del dataset

In uno scenario reale, i dati verrebbero recuperati tramite le **Power BI REST API**.

---

## Architettura
- Script Python che simula una pipeline di ingestione metadata
- Recupero dati (mock API)
- Trasformazione in schema analytics-ready
- Calcolo campo derivato `status`
- Salvataggio su database SQLite
- Aggiornamento tramite logica di upsert (chiave: asset_id)

---

## Perché SQLite
Ho scelto SQLite perché:
- è semplice e leggero
- non richiede setup complesso
- supporta operazioni di insert/update
- è più realistico di un CSV per simulare un layer dati

In produzione userei un data warehouse (es. Databricks, Snowflake).

---

## Modello dati
Tabella: `power_bi_asset_analytics`

Campi principali:
- asset_id
- asset_name
- asset_type
- owner
- workspace_name
- last_updated_at
- last_viewed_at
- views_last_30d
- last_refresh_at
- refresh_status
- refresh_frequency
- status (derivato)
- last_synced_at

---

## Logica dello status
Il campo `status` è derivato:

- **Refresh Failed** → refresh fallito
- **Stale** → non visualizzato da più di 30 giorni
- **Active** → usato recentemente
- **Unknown** → dati insufficienti

---

## Scheduling
In produzione la pipeline girerebbe:
- giornalmente (monitoraggio base)
- o più frequentemente se necessario

Possibili strumenti:
- scheduler
- orchestratori (es. Airflow)
- job cloud

---

## Gestione credenziali
Nel progetto sono simulate.

In produzione:
- environment variables
- secret manager
- niente credenziali hardcoded

---

## Gestione errori
Implementato:
- logging base
- try/except

In produzione aggiungerei:
- retry automatici
- alert su failure
- gestione errori API

---

## Storage e idempotenza
- Chiave primaria: `asset_id`
- Logica: `INSERT OR REPLACE`
- Evita duplicati
- Permette aggiornamenti consistenti

---

## Trasformazioni dati
- normalizzazione campi
- schema consistente
- aggiunta campo `status`
- aggiunta timestamp `last_synced_at`

---

## Monitoring e alerting

### Dashboard stale
- non viste da 30 giorni

### Refresh falliti
- stato refresh = Failed

### Calo utilizzo
- drop di views_last_30d

Alert possibili:
- email
- Slack / Teams
- tool monitoring

---

## Design decision
Ho scelto SQLite invece di CSV perché:
- supporta update
- più vicino a un sistema reale
- evita duplicati
- migliora qualità del dataset

---

## Data Governance

### Metriche consistenti
- glossario centralizzato
- definizioni ufficiali
- owner dei KPI
- dataset certificati

### Dati errati o obsoleti
- tracciamento lineage
- verifica refresh
- audit dashboard

### Metriche diverse tra team
- allineamento stakeholder
- definizione ufficiale
- documentazione
- distinzione se necessario

---

## Dashboard di monitoraggio (idee)

- Active vs Stale vs Failed
- Dashboard non usate
- Refresh falliti per workspace
- Trend utilizzo
- Top dashboard
- Dashboard per owner
- Dashboard per workspace
- Errori recenti

---

## Come eseguire

```bash
python main.py