from datetime import datetime
from uuid import uuid4

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_custom import PostgreSQLCountRows
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

config = {"dag_1": {"schedule_interval": None, "start_date": datetime(2019, 5, 10), "table_name": "record_1"},
          "dag_2": {"schedule_interval": None, "start_date": datetime(2019, 5, 10), "table_name": "record_2"},
          "dag_3": {"schedule_interval": None, "start_date": datetime(2019, 5, 10), "table_name": "record_3"}}


def is_table_exists(schema, table):
    hook = PostgresHook()

    query = f'''
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = '{schema}' 
        AND table_name = '{table}'
        '''
    is_exist = hook.get_first(query)

    return "skip_table_creation" if is_exist else "create_table"


def create_dag(dag_id, schedule_interval, start_date, schema, table):
    with DAG(dag_id=dag_id,
             schedule_interval=schedule_interval,
             start_date=start_date) as dag:
        print_start = PythonOperator(task_id="print_process_start",
                                     python_callable=lambda: print(
                                         f"{dag.dag_id} start processing tables in database {schema}"))

        get_user = BashOperator(task_id="get_user",
                                xcom_push=True,
                                bash_command="whoami")

        check_table_exists = BranchPythonOperator(task_id="check_table_exists",
                                                  python_callable=is_table_exists,
                                                  op_kwargs={'schema': schema, 'table': table})

        create_table = PostgresOperator(task_id="create_table",
                                        sql=f'''CREATE TABLE {schema}.{table}(
                                        id uuid NOT NULL, 
                                        username VARCHAR (50) NOT NULL, 
                                        timestamp TIMESTAMP NOT NULL)''')

        skip_table_creation = DummyOperator(task_id="skip_table_creation")

        insert_new_row = PostgresOperator(task_id="insert_new_row",
                                          sql=f"INSERT INTO {table} VALUES ("
                                              f"'{uuid4()}',"
                                              "'{{ task_instance.xcom_pull(task_ids='get_user') }}',"
                                              f"'{datetime.now()}')",
                                          trigger_rule=TriggerRule.ALL_DONE)

        count_rows = PostgreSQLCountRows(task_id="count_rows",
                                         do_xcom_push=True,
                                         schema=schema,
                                         table=table)

        end = BashOperator(task_id="end",
                           xcom_push=True,
                           bash_command="echo {{ run_id }} ended")

        print_start >> get_user >> check_table_exists >> (
            create_table, skip_table_creation) >> insert_new_row >> count_rows >> end

        return dag


for key, value in config.items():
    globals()[key] = create_dag(dag_id=key,
                                schedule_interval=value["schedule_interval"],
                                start_date=value["start_date"],
                                schema="public",
                                table=value["table_name"])
