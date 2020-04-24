import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.presto_hook import PrestoHook
from datetime import datetime, timedelta
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.dummy_operator import DummyOperator


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 4, 22),
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
    dag_id='presto_hourly_data_original_load',
    default_args=default_args,
    schedule_interval='0 * * * *')


start_job = ExternalTaskSensor(
    task_id='start_job',
    external_dag_id='presto_hourly_data_original_load',
    external_task_id='finish_job',
    allowed_states=['success'],
    execution_delta=timedelta(hours=1),
    dag=dag)





def presto_run_insert_sql(airflow_run_date, airflow_run_hour):
        processing_date = airflow_run_date
        processing_hour = airflow_run_hour.replace('T',' ')[:19]
        presto_hook = PrestoHook(conn_name_attr= 'presto_default')

        sql = """
                 insert into hive.data_signals.data_original_signals
                 select msgId, timestamp, epoch, msgName, signalName, value, date, vin
                 from hive.data_signals.data_signals
                 where date = '{date_param}' and from_unixtime(epoch) between  cast('{hour_param}' as timestamp)
                 and cast('{hour_param}' as timestamp) + interval '1' HOUR """.format(date_param=processing_date,hour_param=processing_hour)


        print(sql)
        data = presto_hook.get_records(sql)
        print('output data = ' + str(data))

presto_run_insert = PythonOperator(
    task_id='presto_run_insert',
    python_callable=presto_run_insert_sql,
    op_kwargs={'airflow_run_date': '{{ ds }}', 'airflow_run_hour': '{{ ts }}'},
    retries=3,
    dag=dag)


finish_job = DummyOperator(
    task_id='finish_job',
    dag=dag
)


start_job >> presto_run_insert >> finish_job
