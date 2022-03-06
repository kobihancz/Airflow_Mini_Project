from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pathlib import Path

def analyze_file(file):
    count = 0
    cur_list = []

    fp = open(file)
    for line in fp:
        if "ERROR -" in line:
            count+=1
            cur_list.append(line)
    fp.close()
    return count, cur_list

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime.now() - timedelta(days=1)
}

dag_log_analyzer = DAG(
dag_id='loganalyzer',
default_args=default_args,
description='DAG to analyzer marketvol dag logs.'
)



