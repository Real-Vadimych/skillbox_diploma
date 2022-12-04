import datetime as dt
import os
import sys

from airflow.models import DAG
from airflow.operators.python import PythonOperator

path = os.path.expanduser('/home/vvk/DataGripProjects/skillbox_diploma')
os.environ['PROJECT_PATH'] = path

sys.path.insert(0, path)

from main import pipeline

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2022, 4, 12),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

with DAG(
        dag_id='sber_autopodpiska_json_2_db',
        schedule_interval="00 15 * * *",
        default_args=args,
) as dag:
    pipeline = PythonOperator(
        task_id='pipeline',
        python_callable=pipeline,
    )

    pipeline