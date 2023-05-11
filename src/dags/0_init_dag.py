import logging
from airflow.operators.python import PythonOperator
from datetime import datetime
import vertica_python  #  для подключения к VERTICA
from airflow import DAG

log = logging.getLogger(__name__)

# VERTICA DESTINATION
conn_args_dest = {
    "host": "51.250.75.20",
    "port": "5433",
    "user": "yamarselr2015yandexru",
    "password": "EWgR2UjZH2TX6LP",
    "database": "dwh",
    "autocommit": True,
}

# Объявляем даг
dag = DAG(
    dag_id="final_project_DDL",
    schedule_interval=None,  #'0/1 * * * *', # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=datetime.today(),  # datetime.now(timezone("UTC"))-timedelta(days=3), # Дата начала выполнения дага
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=["stage", "cdm", "final_project", "ddl"],
    is_paused_upon_creation=True,  # Остановлен/запущен при появлении.
)


# Создаем таблицы в VERTICA (STG)
def exec_ddl_stg(conn_args_dest: conn_args_dest):
    with vertica_python.connect(**conn_args_dest) as conn:
        cur = conn.cursor()
        sql_statement = open("/lessons/sql/stg_ddl.sql").read()
        cur.execute(sql_statement)


# Создаем витрину в VERTICA (CDM)
def exec_ddl_cdm(conn_args_dest: conn_args_dest):
    with vertica_python.connect(**conn_args_dest) as conn:
        cur = conn.cursor()
        sql_statement = open("/lessons/sql/cdm_ddl.sql").read()
        cur.execute(sql_statement)


exec_ddl_stg_dag_job = PythonOperator(
    task_id="exec_ddl_stg", python_callable=exec_ddl_stg, op_kwargs={"conn_args_dest": conn_args_dest}, dag=dag,
)

exec_ddl_cdm_dag_job = PythonOperator(
    task_id="exec_ddl_cdm", python_callable=exec_ddl_cdm, op_kwargs={"conn_args_dest": conn_args_dest}, dag=dag,
)

[exec_ddl_stg_dag_job, exec_ddl_cdm_dag_job]
