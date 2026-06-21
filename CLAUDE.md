# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **Databricks Asset Bundle (DAB)** project named `dab_my_project`. It uses the `default-python` template and targets an Azure Databricks workspace at `adb-7405611495879743.3.azuredatabricks.net`.

The bundle deploys:
- A **Delta Live Tables (DLT) pipeline** (`dab_my_project_etl`) that runs Python transformations
- A **sample job** (`sample_job`) that orchestrates a notebook, a Python wheel task, and the DLT pipeline on a daily schedule

## Development Commands

Install dependencies (requires [uv](https://docs.astral.sh/uv/)):
```
uv sync --dev
```

Run all tests (connects to Databricks via Databricks Connect):
```
uv run pytest 
```

OR use 
```
DATABRICKS_CONFIG_PROFILE=dev uv run pytest
```

Run a single test file:
```
uv run pytest tests/sample_taxis_test.py
```

Lint code (line length: 120):
```
uv run ruff check .
```

Build the Python wheel artifact:
```
uv build --wheel
```

## Databricks CLI Workflow

Authenticate first (one-time):
```
databricks configure
```

Deploy to dev (default target, prefixes resources with `[dev <username>]`, pauses schedules):
```
databricks bundle deploy --target dev
```

Deploy to prod:
```
databricks bundle deploy --target prod
```

Run a specific job or pipeline:
```
databricks bundle run sample_job
databricks bundle run dab_my_project_etl
```

Run a single DLT transformation by name:
```
databricks bundle run dab_my_project_etl --select sample_trips_dab_my_project
```

## Architecture

### Two Python packages in `src/`

**`src/dab_my_project/`** — Shared Python library packaged as a `.whl`, used by the `python_wheel_task` in the job.
- `main.py`: Entry point (`main` CLI script). Accepts `--catalog` and `--schema` args, sets the active catalog/schema, then calls business logic.
- `taxis.py`: Business logic functions (e.g., `find_all_taxis()` reads from `samples.nyctaxi.trips`).

**`src/dab_my_project_etl/`** — DLT pipeline source, uploaded as files (not a wheel). Each file under `transformations/` is a DLT dataset definition.
- `transformations/`: Each `.py` file defines one DLT table using the `@dp.table` decorator from `pyspark.pipelines`. Tables in this folder depend on each other by name (e.g., `sample_zones_dab_my_project` reads from `sample_trips_dab_my_project`).
- `explorations/`: Ad-hoc Jupyter notebooks, not deployed.

### Bundle configuration

- `databricks.yml`: Root bundle config. Declares two targets (`dev`, `prod`), bundle-level variables (`catalog`, `schema`), and the Python wheel artifact build command.
- `resources/*.yml`: Separate YAML files for each Databricks resource:
  - `dab_my_project_etl.pipeline.yml` — DLT pipeline definition (serverless, Unity Catalog)
  - `sample_job.job.yml` — Job with three tasks: notebook → python wheel + DLT pipeline (both in parallel after notebook)

### Variables

`catalog` and `schema` are bundle-level variables resolved per target:
- **dev**: catalog = `my_databricks_workspace`, schema = current user's short name
- **prod**: catalog = `my_databricks_workspace`, schema = `prod`

These are passed as job parameters and referenced via `${var.catalog}` / `${var.schema}` in resource YAMLs.

### Testing

Tests in `tests/` use `databricks-connect` (v15.4) to run Spark code against the remote Databricks cluster. The `conftest.py` provides `spark` and `load_fixture` fixtures. If no compute is configured, it falls back to serverless compute automatically.

Fixture data (JSON/CSV) lives in `fixtures/` and is loaded via the `load_fixture` pytest fixture.

## Ingestions

### JSON Ingestion (`ingestion_req_1`)

Batch JSON ingestion from a Unity Catalog Volume into a Delta table using Spark Structured Streaming (no Auto Loader).

**Python package:** `src/ingestion/dab_my_project_ingestion/`
- `json_ingestion.py`: Entry point (`json_ingestion` CLI script). Accepts `--catalog`, `--schema`, `--table_name`, `--source_folder`. Uses `databricks.sdk.runtime.spark` (not a standalone SparkSession).

**Processing flow:**
1. Builds source path: `/Volumes/<catalog>/test_schema/source_json_volume/<source_folder>/`
2. Infers JSON schema via `spark.read.json(source_path).schema`
3. Opens a streaming read with `readStream.schema(...).json(source_path)` (plain JSON, no `cloudFiles`)
4. Writes via `foreachBatch` → `saveAsTable` in Delta append mode with `mergeSchema=true`
5. Checkpoint path: `/Volumes/<catalog>/test_schema/source_json_volume/checkpoints/<table_name>`
6. Uses `trigger(availableNow=True)` + `awaitTermination()` so the job runs as a scheduled batch

**Bundle resources added:**
- `resources/json_ingestion_job.job.yml` — job with four parameters (`catalog`, `schema`, `table_name`, `source_folder`)
- `resources/source_json_volume.volume.yml` — Unity Catalog Volume (`source_json_volume`) in schema `test_schema`

**Bundle variables added to `databricks.yml`:**
- `table_name` (default: `customers`)
- `source_folder` (default: `customers`)

**`pyproject.toml` changes:**
- Added `json_ingestion` script entry point pointing to `dab_my_project_ingestion.json_ingestion:main`
- Configured hatchling to discover both `src/dab_my_project` and `src/ingestion/dab_my_project_ingestion` in the wheel

**Run the ingestion job:**
```
databricks bundle run json_ingestion_job
```

**Run with custom parameters:**
```
databricks bundle run json_ingestion_job --params '{"table_name": "orders", "source_folder": "orders"}'
```

## Compute Cluster

### Compute for `json_ingestion_job`

The ingestion job uses a dedicated job cluster defined in `resources/json_ingestion_job.job.yml`.

**Cluster spec (`job_clusters` block):**
- Node type: `Standard_D4ds_v5`
- Runtime: Photon, Spark `17.3.x-scala2.13`
- Availability: `ON_DEMAND_AZURE`
- Autoscale: 1–3 workers

### Switching to Serverless

To switch to serverless compute, make these edits in `resources/json_ingestion_job.job.yml`:
1. Remove the `job_clusters` block entirely
2. On the task, replace `job_cluster_key: ingestion_cluster` with `environment_key: default`
3. Add an `environments` block at the job level (commented out template is in the file)
