from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

args = {
    'owner':'airflow',
}


with DAG (
    dag_id='rental-hijo',
    default_args=args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['edvai','final','rental'],
    params={'ejemplo':'ejemplo'},) as dag:
    
    transform = BashOperator(
        task_id='transforma_y_carga',
        bash_command=(
        "ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files "
        "/home/hadoop/hive/conf/hive-site.xml "
        "/home/hadoop/scripts/transformation.py "),
    )
    
    inicio = EmptyOperator(
        task_id='inicio',)
    fin = EmptyOperator(
        task_id='fin',)
    
    inicio >> transform >> fin



