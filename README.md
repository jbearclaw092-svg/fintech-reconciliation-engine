# FinTech Multi-Currency Financial Reconciliation Engine

An end-to-end cloud Analytics Engineering pipeline that ingests internal payment gateway transactions and external European Central Bank (ECB) exchange rates into **Snowflake**, transforms and reconciles multi-currency records using **dbt Core**, orchestrates execution via **Dagster**, and enforces shift-left data quality through **GitHub Actions CI/CD**.

## Architecture

```text
[ECB / FX Rate API] ─────┐
                         ├──> [Python + Dagster] ──> [Snowflake RAW_DB] ──> [dbt Core] ──> [Snowflake ANALYTICS_DB]
[Internal Ledger Logs] ──┘                                                      │
                                                                      [GitHub Actions CI/CD]
```

## Key Engineering Highlights
* **Idempotent Ingestion (`Dagster` + `Pandas` + `PyArrow`):** Extracts external FX rates with defensive HTTP fallback handling and bulk-loads Parquet-serialized DataFrames into Snowflake internal stages via `write_pandas(..., overwrite=True)`.
* **Modular ELT Modeling (`dbt Core`):** Separates raw ingestion (`FINTECH_RAW_DB`) from standardized staging views (`stg_transactions`, `stg_ecb_rates`) and analytical marts (`fct_reconciliation`) using fixed-point `DECIMAL(18,2)` arithmetic to prevent floating-point rounding drift.
* **Automated Anomaly Detection:** Dynamically converts multi-currency transactions (`USD`, `GBP`, `CHF`) to `EUR` using daily market rates and flags conversion discrepancies exceeding a 2% variance threshold (`DISCREPANCY_FLAGGED`).
* **Shift-Left Data Quality & CI/CD:** Enforces `unique`, `not_null`, and `accepted_values` tests on every build and validates pull requests automatically inside an isolated `FINTECH_CI_TEST_DB` environment via GitHub Actions.
