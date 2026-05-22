# Airflow Dynamic DAGs

This project demonstrates an Apache Airflow pipeline that generates DAGs dynamically from a YAML configuration file.

## Overview

- `dags/generate_dynamic_dags.py` reads `dags/pipeline_config.yaml` and creates one DAG per pipeline entry.
- Each generated DAG uses TaskFlow tasks to extract and load data.
- The configuration-driven approach enables new pipelines to be added without changing DAG code.

## DAG behavior

Each generated DAG has:

- `dag_id` in the format: `dynamic_sync_<pipeline_id>`
- a `start_sync` empty task
- an `extract_from_<source_table>` task
- a `load_into_<target_table>` task

The DAG run order is:

```text
start_sync -> extract_from_<source_table> -> load_into_<target_table>
```

## Configuration

`dags/pipeline_config.yaml` defines global defaults and named pipelines.

Example configuration keys:

- `id` — unique pipeline identifier
- `schedule` — cron schedule for the DAG
- `source_table` — source table name for extraction
- `target_table` — destination table name for loading
- `alert_email` — notification email for failures

## Local development

### Run with Astro

```bash
cd /Users/polagani/Documents/airflow-dynamic-dags
astro dev start
```

Then visit:

```text
http://localhost:8080
```

### Validate the DAG

Run Airflow’s DAG parser or compile the DAG directly:

```bash
python3 -m py_compile dags/generate_dynamic_dags.py
```

## Files

- `dags/generate_dynamic_dags.py` — dynamic DAG generator
- `dags/pipeline_config.yaml` — pipeline definitions
- `airflow_settings.yaml` — local Airflow connections and variables
- `Dockerfile` — runtime image configuration
- `requirements.txt` — Python dependencies
- `tests/` — test cases and validation

## Best practices

- Keep pipeline metadata in `pipeline_config.yaml`, not in DAG code
- Use descriptive DAG IDs and task IDs
- Use `schedule=None` only for trigger-only DAGs
- Prefer small config payloads in `dag_run.conf` and larger datasets via external storage


