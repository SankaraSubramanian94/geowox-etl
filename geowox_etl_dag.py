from airflow import DAG
from airflow.operators import BashOperator
import os
from datetime import datetime, timedelta

yesterday = datetime.combine(
    datetime.today() - timedelta(1),
    datetime.min.time())

args = {
    'owner': 'Airflow',
    'start_date': yesterday,
	'depends_on_past': False,
	'email': ['x18179541@student.ncirl.ie'],
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 1,
	'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    dag_id='geowox_etl_pipeline',
    default_args=args,
    schedule_interval=None    
)

# Run extract task
extract = BashOperator(
    task_id='extract',
    bash_command='python3 /home/airflow/Final_codes/extract.py "https://www.propertypriceregister.ie/"',
    dag=dag)

# Run transform task
transform = BashOperator(
    task_id='transform',
    bash_command='python3 /home/airflow/Final_codes/transform.py',
    dag=dag)
	
# Run load task
load = BashOperator(
    task_id='load',
    bash_command='python3 /home/airflow/Final_codes/load.py',
    dag=dag)

# set dependencies
extract >> transform >> load
