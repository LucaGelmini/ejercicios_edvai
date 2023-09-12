from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

def push_filenames_Xcom(**kwargs):
    filename1 = "CarRentalData.csv"
    filename2 = "CarRentalGeo.csv"
    kwargs['ti'].xcom_push(key="carrental_file1",value=filename1)
    kwargs['ti'].xcom_push(key="carrental_file2",value=filename2)


args = {
    'owner':'airflow',
}

with DAG (
    dag_id='rental-padre',
    default_args=args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['edvai','final','rental']) as dag:
    
    inicio = EmptyOperator(
        task_id='inicio',)
    fin = EmptyOperator(
        task_id='fin',)
    
    ingest  = BashOperator(
        task_id='ingest',
        bash_command='/usr/bin/sh /home/hadoop/scripts/ingest-informe-rental.sh '
        )
    
    push_xcom = PythonOperator(
        task_id='push_filenames_xcom', 
        python_callable=push_filenames_Xcom,
        provide_context=True,
    )
    


    trigger_child = TriggerDagRunOperator(
        task_id = 'dispara_dag_hijo',
        trigger_dag_id="rental-hijo",
        execution_date= '{{ ds }}',
        reset_dag_run=True,
    )

    inicio >> ingest  >> push_xcom >> trigger_child >> fin
