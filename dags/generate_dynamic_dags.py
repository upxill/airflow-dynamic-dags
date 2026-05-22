# dags/generate_dynamic_dags.py
import os
import yaml
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

# 1. Locate and parse the configuration file securely
CONFIG_FILE_PATH = os.path.join(os.path.dirname(__file__), "pipeline_config.yaml")

if not os.path.exists(CONFIG_FILE_PATH):
    raise FileNotFoundError(f"Missing dynamic DAG configuration: {CONFIG_FILE_PATH}")

with open(CONFIG_FILE_PATH, "r") as file:
    config = yaml.safe_load(file) or {}

# Pull global settings
defaults = config.get("global_defaults", {})
pipelines = config.get("pipelines", [])

if not isinstance(pipelines, list):
    raise ValueError(
        "The pipeline_config.yaml file must define a list under 'pipelines'."
    )


# 2. Define a factory function to encapsulate DAG creation logic
def create_dag(pipeline_id, schedule, source, target, email):

    default_args = {
        "owner": defaults.get("owner", "airflow"),
        "retries": defaults.get("retries", 1),
        "retry_delay": timedelta(minutes=5),
        "email": [email] if email else [],
        "email_on_failure": True,
    }

    # Construct a distinct DAG object for each pipeline configuration
    with DAG(
        dag_id=f"dynamic_sync_{pipeline_id}",
        description=f"Dynamic sync pipeline for {pipeline_id}.",
        default_args=default_args,
        schedule=schedule,
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["dynamic", "automated"],
    ) as dag:
        start = EmptyOperator(task_id="start_sync")

        @task(task_id=f"extract_from_{source}")
        def extract_table(src_table: str):
            print(f"Extracting data from database table: {src_table}")
            return f"/tmp/extract_{src_table}.csv"

        @task(task_id=f"load_into_{target}")
        def load_table(file_path: str, tgt_table: str):
            print(f"Reading data from {file_path}")
            print(f"Loading data into destination table: {tgt_table}")

        extracted_file = extract_table(source)
        end_load = load_table(extracted_file, target)

        start >> extracted_file >> end_load

    return dag


# 3. Iterate through configs and inject generated DAGs into the global scope
for pipe in pipelines:
    generated_dag = create_dag(
        pipeline_id=pipe["id"],
        schedule=pipe["schedule"],
        source=pipe["source_table"],
        target=pipe["target_table"],
        email=pipe.get("alert_email", ""),
    )

    # Airflow requires the DAG variable name to match the dag_id inside globals()
    globals()[generated_dag.dag_id] = generated_dag
