-- Create banking data warehouse database
CREATE DATABASE banking_dw;

\c banking_dw;

-- Raw landing table
CREATE TABLE IF NOT EXISTS raw_transactions (
    transaction_id      VARCHAR(64),
    client_id           VARCHAR(64),
    date_transaction    VARCHAR(32),
    montant             NUMERIC(18,4),
    devise              VARCHAR(10),
    taux_change_eur     NUMERIC(10,6),
    montant_eur         NUMERIC(18,4),
    categorie           VARCHAR(100),
    produit             VARCHAR(100),
    agence              VARCHAR(100),
    type_operation      VARCHAR(50),
    statut              VARCHAR(50),
    score_credit_client NUMERIC(6,2),
    segment_client      VARCHAR(50),
    solde_avant         NUMERIC(18,4),
    taux_interet        NUMERIC(8,4),
    ingested_at         TIMESTAMP DEFAULT NOW()
);

-- Cleaned fact table
CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id      VARCHAR(64)   PRIMARY KEY,
    client_id           VARCHAR(64)   NOT NULL,
    date_transaction    DATE          NOT NULL,
    montant             NUMERIC(18,4) NOT NULL,
    devise              VARCHAR(10),
    taux_change_eur     NUMERIC(10,6),
    montant_eur         NUMERIC(18,4),
    categorie           VARCHAR(100),
    produit             VARCHAR(100),
    agence              VARCHAR(100),
    type_operation      VARCHAR(50),
    statut              VARCHAR(50),
    score_credit_client NUMERIC(6,2),
    segment_client      VARCHAR(50),
    solde_avant         NUMERIC(18,4),
    solde_apres         NUMERIC(18,4),
    taux_interet        NUMERIC(8,4),
    is_debit            BOOLEAN,
    processed_at        TIMESTAMP DEFAULT NOW()
);

-- Clients dimension
CREATE TABLE IF NOT EXISTS dim_clients (
    client_id           VARCHAR(64)   PRIMARY KEY,
    segment_client      VARCHAR(50),
    score_credit_client NUMERIC(6,2),
    first_seen          DATE,
    last_seen           DATE,
    total_transactions  INT DEFAULT 0,
    updated_at          TIMESTAMP DEFAULT NOW()
);

-- Daily aggregation
CREATE TABLE IF NOT EXISTS agg_daily_summary (
    summary_date        DATE,
    categorie           VARCHAR(100),
    agence              VARCHAR(100),
    nb_transactions     INT,
    total_montant_eur   NUMERIC(20,4),
    avg_score_credit    NUMERIC(6,2),
    nb_debits           INT,
    nb_credits          INT,
    PRIMARY KEY (summary_date, categorie, agence)
);