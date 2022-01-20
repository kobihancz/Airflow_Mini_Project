from datetime import date
from datetime import datetime 
from datetime import timedelta
import yfinance as yf
import pandas as pd

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

def download_TSLA_data():
    start_date = date.today()
    end_date = start_date + timedelta(days=1)
    tsla_df = yf.download('TSLA', start=start_date, end=end_date, interval='1d' )
    tsla_df.to_csv('/opt/airflow/tmp/data/' + str(start_date) + "/tsla_data.csv", header=False)

def download_AAPL_data():
    start_date = date.today()
    end_date = start_date + timedelta(days=1)
    aapl_df = yf.download('AAPL', start=start_date, end=end_date, interval='1d' )
    aapl_df.to_csv('/opt/airflow/tmp/data/' + str(start_date) + "/aapl_data.csv", header=False)

def query_data(*args):
    for file in args:
        start_date = date.today()
        filepath = '/airflow/tmp/data/' + str(start_date) + '/' + file
        df = pd.read_csv(filepath, header=None)
        timestamp = df[0]
        avg_price = (df[1]+df[2]+df[3]+df[4]+df[5])/5
        result = [file, timestamp, avg_price]
        print(result)

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime.now() - timedelta(days=1),
    'schedule_interval': '20 2 * * 1-5', 
    'retries': 2,
    'retry_delay': timedelta(minutes=5) 
}

finance_dag = DAG(
    dag_id='marketvol',
    default_args=default_args,
    description='A simple DAG'
)

t0 = BashOperator(
    task_id='create_tmp_folder',
    bash_command='mkdir -p airflow/tmp/data/$(date +%Y-%m-%d)',
    dag=finance_dag
)

t1 = PythonOperator(
    task_id='download_aapl_data',
    python_callable=download_AAPL_data,
    provide_context=True,
    dag=finance_dag,
)

t2 = PythonOperator(
    task_id='download_tsla_data',
    python_callable=download_TSLA_data,
    provide_context=True,
    dag=finance_dag,
)

t5 = PythonOperator(
    task_id='query_data',
    python_callable=query_data,
    op_args=['aapl_data.csv', 'tsla_data.csv'],
    provide_context=True,
    dag=finance_dag
)

t0 >> t1 
t0 >> t2
[t1,t2] >> t5
