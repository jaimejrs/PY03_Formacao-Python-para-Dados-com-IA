from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def hello_world():
    print("Olá, Apache Airflow! A DAG está funcionando corretamente.")
    

default_args = {
    'owner': 'airflow',                     
    'retries': 1,                           
    'retry_delay': timedelta(minutes=5),    
}

with DAG(
    dag_id='exemplo_dag',                   
    default_args=default_args,              
    description='Uma DAG de exemplo no Airflow',
    schedule=timedelta(days=1),        
    start_date=datetime(2025, 1, 30),       
    catchup=False                           
) as dag:

    task_hello = PythonOperator(
        task_id='diga_ola',                 
        python_callable=hello_world          
    )

    task_hello