# Technical Assignment | Testini Giuseppe

## Panoramica
Questo progetto simula una pipeline di ingestion dei metadati relativi a dashboard e report, estratti da Power BI Service.

L’obiettivo è costruire un dataset analytics-ready utile per monitorare:
- utilizzo di dashboard e report
- asset non utilizzati o obsoleti
- refresh falliti
- stato generale degli asset BI

Per semplicità, in questa implementazione i dati sono simulati tramite mock metadata.

---

## Scelta della piattaforma
Ho scelto **Power BI** perché è la piattaforma con cui ho maggiore familiarità e su cui ho lavorato direttamente.

Questo mi ha permesso di focalizzarmi su:
- modellazione dei metadata
- costruzione di un dataset analytics-ready
- logiche di monitoraggio BI

In un contesto reale, i dati verrebbero recuperati tramite le **Power BI REST API**.

---

## Architettura
- Script Python eseguito su base schedulata
- Recupero metadata da Power BI (simulato con mock data)
- Trasformazione in uno schema analytics-ready coerente
- Calcolo di uno `status` derivato per monitoraggio operativo
- Persistenza dei dati in SQLite
- Export finale anche in CSV per rendere l’output facilmente leggibile e ispezionabile

---

## Perché SQLite
Ho scelto **SQLite** come storage principale perché:
- è leggero e locale
- non richiede setup aggiuntivi
- permette di gestire insert/update più facilmente rispetto a un CSV
- rappresenta meglio un piccolo layer analytics-ready

Ho aggiunto anche un **export CSV** per rendere il risultato facilmente leggibile su GitHub.

---

## Modello dati
Tabella principale: `power_bi_asset_analytics`

Campi principali:
- `asset_id`
- `asset_name`
- `asset_type`
- `owner`
- `workspace_name`
- `last_updated_at`
- `last_viewed_at`
- `views_last_30d`
- `last_refresh_at`
- `refresh_status`
- `refresh_frequency`
- `status`
- `last_synced_at`

---

## Logica dello status
Il campo `status` viene calcolato/derivate basandosi su queste regole:

- **Refresh Failed** → se l’ultimo refresh è fallito
- **Stale** → se l’asset non è stato visualizzato da oltre 30 giorni
- **Active** → se l’asset è stato visualizzato recentemente e non ha problemi di refresh
- **Unknown** → se i metadata non sono sufficienti per una classificazione affidabile

---

## Scheduling
In produzione, questa pipeline potrebbe essere eseguita:
- giornalmente per monitoraggio operativo standard
- più frequentemente se servisse maggiore tempestività

L’orchestrazione potrebbe essere fatta con:
- scheduler di sistema
- job cloud

---

## Gestione credenziali
In questa soluzione i metadata sono simulati, quindi non è stata implementata autenticazione reale.

In una versione produttiva:
- le credenziali non sarebbero hardcoded
- verrebbero gestite tramite environment variables o secret manager/scope (come su Databricks ad esempio)
- la chiamata API sarebbe autenticata tramite token

---

## Retry e gestione errori
La soluzione include:
- logging base
- gestione errori tramite `try/except`

In produzione aggiungerei:
- retry automatici per failure temporanei
- timeout handling
- alert su failure ripetute
- monitoraggio più dettagliato dei run

---

## Dataset design
Il dataset utilizza `asset_id` come chiave primaria.

Questo permette una logica di upsert:
- inserimento di nuovi asset
- aggiornamento di asset già esistenti
- nessuna duplicazione

---

## Trasformazioni dati
Le trasformazioni principali includono:
- rinomina dei campi raw in nomi coerenti
- costruzione di uno schema analytics-ready
- aggiunta del campo derivato `status`
- aggiunta del timestamp tecnico `last_synced_at`

Questo rende il dataset riutilizzabile per:
- monitoraggio BI
- dashboard
- analisi stale/failure
- reporting operativo

---

## Monitoring e alerting
In un contesto reale, userei questo dataset per rilevare e notificare:

### Dashboard stale
- asset non visualizzati da più di 30 giorni

### Refresh falliti
- asset con ultimo refresh fallito
- eventuale concentrazione di problemi in un workspace specifico

### Calo di utilizzo
- drop significativo delle visualizzazioni
- confronto con periodi precedenti o soglie storiche

Le notifiche potrebbero essere inviate tramite:
- email
- Slack / Teams
- sistemi di monitoraggio interni

---

## Design decision
Una decisione importante è stata usare **SQLite come storage principale** e **CSV come output leggibile**.

Ho scelto questa combinazione perché:
- SQLite simula meglio uno storage analytics-ready rispetto a un CSV puro
- il CSV rende il risultato facilmente consultabile e versionabile
- la soluzione resta semplice ma credibile per una simulazione tecnica

---

## Data Governance

### 1. Come garantire definizioni coerenti delle metriche?
- mantenere un glossario o catalogo centrale dei KPI
- assegnare owner chiari alle metriche
- riutilizzare semantic layer o dataset certificati
- documentare definizioni, filtri e logiche di calcolo
- validare le nuove dashboard rispetto alle definizioni ufficiali

### 2. Come rilevare dashboard che usano sorgenti errate o obsolete?
- mantenere il lineage tra dashboard, dataset e sorgenti
- controllare timestamp di refresh e validità delle sorgenti
- identificare dashboard collegate a dataset deprecati o non certificati
- fare audit periodici delle sorgenti usate dalle dashboard

### 3. Come gestire metriche definite diversamente da team diversi?
- allineare gli stakeholder sul significato di business
- definire una versione ufficiale condivisa
- documentare la definizione approvata
- distinguere esplicitamente eventuali metriche diverse se i casi d’uso lo richiedono
- aggiornare le dashboard per riflettere la definizione approvata

---

## Dashboard di monitoraggio BI
Se costruissi una dashboard di monitoraggio su questo dataset, includerei:

- numero totale di asset BI
- ripartizione Active / Stale / Refresh Failed
- elenco degli asset non visualizzati negli ultimi 30 giorni
- numero di refresh falliti per workspace
- trend delle visualizzazioni
- top asset per utilizzo
- breakdown per owner
- breakdown per workspace

Ogni metrica è utile per capire:
- adozione
- stato operativo
- ownership