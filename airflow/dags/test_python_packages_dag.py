from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'Sartaj',
    'retry': 5,
    'retry_delay': timedelta(minutes=5)
}


def func1():
    print("In func2")

def func2():
    print("In func2")


with DAG(
    default_args=default_args,
    dag_id="dag_with_python_dependencies_v03",
    start_date=datetime(2021, 10, 12),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='get_sklearn',
        python_callable=func1
    )
    
    task2 = PythonOperator(
        task_id='get_matplotlib',
        python_callable=func2
    )

    task1 >> task2