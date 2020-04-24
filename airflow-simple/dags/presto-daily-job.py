import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.presto_hook import PrestoHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from datetime import datetime, timedelta
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.dummy_operator import DummyOperator


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 3, 25),
    'end_date': datetime(2030, 12, 12),
    'email': ['aashray@ms.com','97675d29.test.onmicrosoft.com@amer.teams.ms'],
    'email_on_failure': True,
    'email_on_retry': True,
    'wait_for_downstream': True,
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    }

dag = DAG(
    dag_id='presto_daily_load',
    default_args=default_args,
    schedule_interval='30 1 * * *')


start_job = ExternalTaskSensor(
    task_id='start_job',
    external_dag_id='presto_daily_load',
    external_task_id='finish_job',
    allowed_states=['success'],
    execution_delta=timedelta(days=1),
    dag=dag)


def presto_dete_before_insert_sql(airflow_run_date):
        processing_date = airflow_run_date
        presto_hook = PrestoHook(conn_name_attr= 'presto_default')
        sql =   """delete from hive.data_signals.data_original_signals
                   where date = '{date_param}' """.format(date_param=processing_date)
        print(sql)
        data = presto_hook.get_records(sql)
        print('output data = ' + str(data))

presto_dete_before_insert = PythonOperator(
    task_id='presto_dete_before_insert',
    python_callable=presto_dete_before_insert_sql,
    op_kwargs={'airflow_run_date': '{{ ds }}'},
    retries=3,
    dag=dag)


delete_aws_s3_folder_cmd = """ aws s3 rm s3://test-data--signals/data-signals/   --recursive --exclude "*" --include date={{ds}}* """

delete_aws_s3_folder = SSHOperator(
    task_id='delete_aws_s3_folder',
    ssh_conn_id='ssh_emr_prod',
    command=delete_aws_s3_folder_cmd,
    dag=dag)


ssh_hive_repair_table_cmd = """ sudo hive -e  "msck repair table data_signals.data_original_signals" """
ssh_hive_repair_table = SSHOperator(
    task_id='ssh_hive_repair_table',
    ssh_conn_id='ssh_emr_prod',
    command=ssh_hive_repair_table_cmd,
    dag=dag)



def presto_run_insert_sql(airflow_run_date):
        processing_date = airflow_run_date
        presto_hook = PrestoHook(conn_name_attr= 'presto_default')
        sql =   """insert into hive.data_signals.data_original_signals
                   select msgId, timestamp, epoch, msgName, signalName, value, date, vin
                   from hive.data_signals.data_signals  where date = '{date_param}' """.format(date_param=processing_date)
        print(sql)
        data = presto_hook.get_records(sql)
        print('output data = ' + str(data))

presto_run_insert = PythonOperator(
    task_id='presto_run_insert',
    python_callable=presto_run_insert_sql,
    op_kwargs={'airflow_run_date': '{{ ds }}'},
    retries=3,
    dag=dag)


finish_job = DummyOperator(
    task_id='finish_job',
    dag=dag
)


start_job >> presto_dete_before_insert >> delete_aws_s3_folder >> ssh_hive_repair_table >> presto_run_insert >> finish_job
