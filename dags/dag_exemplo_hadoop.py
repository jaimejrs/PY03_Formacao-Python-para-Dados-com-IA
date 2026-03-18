from datetime import datetime, timedelta
from io import BytesIO

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from hdfs import InsecureClient


def enviar_dados_para_hdfs() -> None:
    hdfs_url = "http://host.docker.internal:9870"
    hdfs_user = "root"
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    hdfs_path = f"/data/dados_{timestamp}.csv"

    data = {
        "id": [1, 2, 3],
        "nome": ["Alice", "Bob", "Charlie"],
        "idade": [25, 30, 35],
    }
    df = pd.DataFrame(data)

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    client = InsecureClient(hdfs_url, user=hdfs_user)

    with BytesIO(csv_bytes) as reader:
        client.write(hdfs_path, reader, overwrite=True)

    print(f"Dados enviados para o HDFS em: {hdfs_path}")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="exemplo_hadoop",
    default_args=default_args,
    description="Envia um CSV de exemplo para HDFS a cada 5 minutos",
    schedule="*/5 * * * *",
    start_date=datetime(2025, 1, 30),
    catchup=False,
) as dag:
    tarefa_enviar_hdfs = PythonOperator(
        task_id="enviar_dados_hdfs",
        python_callable=enviar_dados_para_hdfs,
    )

    tarefa_enviar_hdfs
