# dbt Transformation Layer

The transformation layer for this project: Bronze -> Silver (staging
models: dedup, typing) -> Gold (mart models: joins, denormalization,
star schema), targeting Databricks via `dbt-databricks`, with tests,
auto-generated documentation, and a lineage graph.

## Setup

```bash
pip install dbt-core dbt-databricks

mkdir -p ~/.dbt
cp profiles.yml.sample ~/.dbt/profiles.yml
# fill in host / http_path / token from your Databricks workspace
```

`profiles.yml` is intentionally kept outside this folder (and out of git)
since it's where connection credentials live — dbt always looks for it at
`~/.dbt/profiles.yml` regardless of which project you're running.

## Project layout

```
healthcare_dbt/
├── dbt_project.yml
├── macros/
│   └── generate_schema_name.sql   -- keeps models in exact "silver"/"gold" schemas
└── models/
    ├── staging/
    │   ├── _staging__sources.yml   -- declares bronze.* as dbt sources
    │   ├── _staging__models.yml    -- tests + docs for staging models
    │   └── stg_*.sql                -- one per raw table, same cleanup as sql/02
    └── marts/
        ├── _marts__models.yml      -- tests + docs for mart models
        ├── dim_date.sql
        ├── dim_patient.sql
        ├── dim_doctor.sql
        ├── dim_medication.sql
        ├── dim_procedure.sql
        ├── fact_prescription.sql
        └── fact_hospital_visit.sql
```

## Running it

This assumes `bronze.*` already has data loaded (via
`databricks/notebooks/01_bronze_ingest.py`, as in the main pipeline setup).

```bash
cd healthcare_dbt

dbt debug          # verify the connection works
dbt run            # build all staging views + mart tables
dbt test           # run all unique/not_null/relationships/accepted_values tests
dbt docs generate  # build documentation + lineage graph
dbt docs serve     # opens an interactive lineage graph + column-level docs in your browser
```

`dbt docs serve` is worth doing even just once — it renders a clickable
DAG of every model, shows which columns feed into which downstream model,
and is genuinely useful to screenshot for a portfolio README.

## What the tests actually check

- **`unique` / `not_null`** on every primary key (`patient_id`,
  `prescription_id`, `visit_id`, etc.)
- **`relationships`** — every foreign key in a fact table is checked
  against its dimension (e.g. every `fact_hospital_visit.doctor_id` must
  exist in `dim_doctor.doctor_id`). This is the automated version of the
  cascading-FK-failure bug documented in the main README's "Challenges"
  section (from the original Postgres build) — with these tests in
  place, `dbt test` would have caught that bug immediately with a clear
  failure message, instead of it surfacing as a silent 0-row table three
  steps downstream.
- **`accepted_values`** — `sex` and `age_group` are checked against known
  valid values, catching bad data at the model level rather than letting
  it flow through to analytics queries.

## Note on materialization

Staging models are `view`s (fast to build, always reflect current raw
data). Mart models are `table`s (materialized, since dashboards querying
`fact_hospital_visit` repeatedly shouldn't re-run the joins every time).
This is configured centrally in `dbt_project.yml`, not per-model.
