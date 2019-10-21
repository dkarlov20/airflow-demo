from datetime import datetime

from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

file_path = Variable.get("file_path") or "/usr/local/airflow/shared/"
triggered_dag = Variable.get("triggered_dag") or "dag_1"


def pull_from_xcom(**kwargs):
    print(kwargs['ti'].xcom_pull(dag_id=triggered_dag, task_ids="end"))
    print(kwargs)


def create_sub_dag(parent_dag_id, parent_task_id, schedule_interval, start_date):
    with DAG(dag_id=f"{parent_dag_id}.{parent_task_id}",
             schedule_interval=schedule_interval,
             start_date=start_date) as sub_dag:
        external_dag_monitor = ExternalTaskSensor(task_id="external_dag_monitor",
                                                  external_dag_id=triggered_dag,
                                                  external_task_id=None,
                                                  poke_interval=10)

        print_result = PythonOperator(task_id="print_result",
                                      provide_context=True,
                                      python_callable=pull_from_xcom)

        remove_trigger_file = BashOperator(task_id="remove_trigger_file",
                                           bash_command=f"rm -f {file_path}/*")

        create_finished_file = BashOperator(task_id="create_finished_file",
                                            bash_command="touch {{ params.finish_path }}/finished_{{ ts_nodash }}",
                                            params={"finish_path": file_path})

        external_dag_monitor >> print_result >> remove_trigger_file >> create_finished_file

    return sub_dag


with DAG(dag_id="trigger_dag",
         schedule_interval=None,
         start_date=datetime(2019, 5, 10)) as dag:
    wait_trigger_file = FileSensor(task_id="wait_trigger_file",
                                   filepath=file_path,
                                   poke_interval=10)

    trigger_dag = TriggerDagRunOperator(task_id="trigger_dag",
                                        trigger_dag_id=triggered_dag,
                                        execution_date='{{ execution_date }}')

    monitor_dag = SubDagOperator(
        task_id="monitor_dag",
        subdag=create_sub_dag(parent_dag_id=dag.dag_id,
                              parent_task_id="monitor_dag",
                              schedule_interval=dag.schedule_interval,
                              start_date=dag.start_date))

    wait_trigger_file >> trigger_dag >> monitor_dag
