import logging
import json
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
from pathlib import Path

import psycopg2
import vertica_python
from airflow import DAG

log = logging.getLogger(__name__)


# POSTGRES (SOURCE)
conn_args_source = {
    "host": "rc1b-w5d285tmxa8jimyn.mdb.yandexcloud.net",
    "port": "6432",
    "dbname": "db1",
    "login": "student",
    "password": "de_student_112022",
}
pg_conn = psycopg2.connect(
    f"dbname={conn_args_source.get('dbname')} host={conn_args_source.get('host')} password={conn_args_source.get('password')} user={conn_args_source.get('login')} port={conn_args_source.get('port')}"
)

# VERTICA (DESTINATION)
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
dag = DAG("final_project_STG", tags=["stg", "loading"], default_args=default_args, schedule_interval="@daily")


# Забираем данные из POSTGRES
def get_data_from_source(pg_conn: pg_conn, table_name: str, orderby: str, **context):
    execution_date = context["logical_date"].strftime("%Y-%m-%d")
    log.info(f"Execution date is {execution_date}")

    # Создаем подключение к источнику Postgres
    with pg_conn.cursor() as cur:
        cur.execute(
            f"""
                SELECT * FROM public.{table_name} WHERE to_char({orderby},'yyyy-mm-dd') ='{execution_date}';
            """
        )
        res = cur.fetchall()
        if res:
            objs = json.loads(json.dumps(res, default=str))

            # Вносим данные в df и сохраняем в csv
            if table_name == "currencies":
                df = pd.DataFrame(
                    objs, columns=["date_update", "currency_code", "currency_code_with", "currency_code_div",],
                )

            elif table_name == "transactions":
                df = pd.DataFrame(
                    objs,
                    columns=[
                        "operation_id",
                        "account_number_from",
                        "account_number_to",
                        "currency_code",
                        "country",
                        "status",
                        "transaction_type",
                        "amount",
                        "transaction_dt",
                    ],
                )
            df.to_csv(f"/data/{table_name}_{execution_date}.csv", index=False)
            context["ti"].xcom_push(key="execution_date", value=execution_date)


# Загружаем данные в VERTICA
def load_data_to_dest(conn_args_dest: conn_args_dest, table_name: str, **context):

    execution_date = context["ti"].xcom_pull(key="execution_date")
    execution_date_under = str(execution_date).replace("-", "_")

    # Если файл CSV есть, то грузим его в STG
    my_file = Path(f"/data/{table_name}_{execution_date}.csv")
    if my_file.is_file():
        with vertica_python.connect(**conn_args_dest) as conn:
            cur = conn.cursor()
            if table_name == "currencies":

                # Грузим данные в БД
                print(f"COPY data to table currencies from {table_name}_{execution_date}.csv")
                cur.execute(
                    f"""
                        COPY YAMARSELR2015YANDEXRU__STAGING.currencies (date_update, currency_code, currency_code_with, currency_code_div)
                        FROM LOCAL '/data/{table_name}_{execution_date}.csv'
                        DELIMITER ',';
                    """
                )

                # Проверяем кол-во записанных в БД строк
                log.info(
                    "COUNT in currencies table:"
                    + str(
                        cur.execute("""SELECT COUNT(*) FROM YAMARSELR2015YANDEXRU__STAGING.currencies;""").fetchall()[
                            0
                        ][0]
                    )
                )

                # Созаем временную таблицу
                log.info(f"CREATE temporary table {table_name}_{execution_date_under}")
                sql_statement = (
                    open("/lessons/sql/create_temp_table_curs.sql")
                    .read()
                    .replace("table_name", f"YAMARSELR2015YANDEXRU__STAGING.{table_name}_{execution_date_under}")
                )
                cur.execute(sql_statement)

                # Обновляем данные в таблице
                log.info(f"MERGE to table {table_name}_{execution_date_under}")
                sql_statement = (
                    open("/lessons/sql/merge_table_curs.sql")
                    .read()
                    .replace("table_name", f"YAMARSELR2015YANDEXRU__STAGING.{table_name}_{execution_date_under}")
                )
                cur.execute(sql_statement)

                # Проверяем кол-во записанных в БД строк после обновления
                log.info(
                    "COUNT in currencies table after merge:"
                    + str(
                        cur.execute("""SELECT COUNT(*) FROM YAMARSELR2015YANDEXRU__STAGING.currencies;""").fetchall()[
                            0
                        ][0]
                    )
                )

            elif table_name == "transactions":

                # Грузим данные в БД
                log.info(f"COPY data to table transaction from {table_name}_{execution_date}.csv")
                cur.execute(
                    f"""
                        COPY YAMARSELR2015YANDEXRU__STAGING.transaction (operation_id, account_number_from, account_number_to, currency_code, country, status, transaction_type, amount, transaction_dt)
                        FROM LOCAL '/data/{table_name}_{execution_date}.csv'
                        DELIMITER ',';
                    """
                )

                # Проверяем кол-во записанных в БД строк
                log.info(
                    "COUNT in transaction table:"
                    + str(
                        cur.execute("""SELECT COUNT(*) FROM YAMARSELR2015YANDEXRU__STAGING.transaction;""").fetchall()[
                            0
                        ][0]
                    )
                )

                # Созаем временную таблицу
                log.info(f"CREATE temporary table {table_name}_{execution_date_under}")
                sql_statement = (
                    open("/lessons/sql/create_temp_table_trans.sql")
                    .read()
                    .replace("table_name", f"YAMARSELR2015YANDEXRU__STAGING.{table_name}_{execution_date_under}")
                )
                cur.execute(sql_statement)

                # Обновляем данные в таблице
                log.info(f"MERGE to table {table_name}_{execution_date_under}")
                sql_statement = (
                    open("/lessons/sql/merge_table_trans.sql")
                    .read()
                    .replace("table_name", f"YAMARSELR2015YANDEXRU__STAGING.{table_name}_{execution_date_under}")
                )
                cur.execute(sql_statement)

                # Проверяем кол-во записанных в БД строк после обновления
                log.info(
                    "COUNT in transaction table after merge:"
                    + str(
                        cur.execute("""SELECT COUNT(*) FROM YAMARSELR2015YANDEXRU__STAGING.transaction;""").fetchall()[
                            0
                        ][0]
                    )
                )

            # Удаляем временную таблицу
            log.info(f"DROP temporary table {table_name}_{execution_date_under}")
            sql_statement = (
                open("/lessons/sql/drop_temp_table.sql")
                .read()
                .replace("table_name", f"YAMARSELR2015YANDEXRU__STAGING.{table_name}_{execution_date_under}")
            )
            cur.execute(sql_statement)

            # Удаляем CSV
            log.info(f"REMOVE /data/{table_name}_{execution_date}.csv")
            try:
                os.remove(f"/data/{table_name}_{execution_date}.csv")
            except OSError:
                pass


get_currencies_from_source_dag_job = PythonOperator(
    task_id="get_currencies_from_source",
    python_callable=get_data_from_source,
    op_kwargs={"pg_conn": pg_conn, "table_name": "currencies", "orderby": "date_update",},
    provide_context=True,
    dag=dag,
)

get_transactions_from_source_dag_job = PythonOperator(
    task_id="get_transactions_from_source",
    python_callable=get_data_from_source,
    op_kwargs={"pg_conn": pg_conn, "table_name": "transactions", "orderby": "transaction_dt",},
    provide_context=True,
    dag=dag,
)

load_currencies_to_dest_dag_job = PythonOperator(
    task_id="load_currencies_to_dest",
    python_callable=load_data_to_dest,
    op_kwargs={"conn_args_dest": conn_args_dest, "table_name": "currencies",},
    provide_context=True,
    dag=dag,
)

load_transactions_to_dest_dag_job = PythonOperator(
    task_id="load_transactions_to_dest",
    python_callable=load_data_to_dest,
    op_kwargs={"conn_args_dest": conn_args_dest, "table_name": "transactions",},
    provide_context=True,
    dag=dag,
)

[get_currencies_from_source_dag_job >> load_currencies_to_dest_dag_job]
[get_transactions_from_source_dag_job >> load_transactions_to_dest_dag_job]
