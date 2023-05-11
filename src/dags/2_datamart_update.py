import logging
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import vertica_python
from airflow import DAG

log = logging.getLogger(__name__)


# VERTICA
conn_args_dest = {
    "host": "51.250.75.20",
    "port": "5433",
    "user": "yamarselr2015yandexru",
    "password": "EWgR2UjZH2TX6LP",
    "database": "dwh",
    "autocommit": True,
}

# Объявляем даг
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 10, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}
dag = DAG("final_project_CDM", tags=["cdm", "datamart"], default_args=default_args, schedule_interval="@daily")


# Обновление витрины global_metrics
def update_global_metrics_mart(conn_args_dest: conn_args_dest, **context):
    execution_date = (context["logical_date"] - timedelta(days=1)).strftime("%Y-%m-%d")
    log.info(f"Execution date is {execution_date}")
    with vertica_python.connect(**conn_args_dest) as conn:
        cur = conn.cursor()

        log.info(f"CLEAR datamart for {execution_date}")
        sql_statement = open("/lessons/sql/clear_datamart.sql").read().replace("execution_date", execution_date)
        cur.execute(sql_statement)

        log.info(f"UPDATE datamart with {execution_date}")
        sql_statement = open("/lessons/sql/update_datamart.sql").read().replace("{execution_date}", execution_date)
        cur.execute(sql_statement)


update_global_metrics_mart_dag_job = PythonOperator(
    task_id="update_global_metrics_mart",
    python_callable=update_global_metrics_mart,
    op_kwargs={"conn_args_dest": conn_args_dest},
    provide_context=True,
    dag=dag,
)

update_global_metrics_mart_dag_job
