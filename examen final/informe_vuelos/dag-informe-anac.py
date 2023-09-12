from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

args = {
        'owner':'airflow',
}

with DAG (
        dag_id='informe-anac',
        default_args=args,
        schedule_interval='0 0 * * *',
        start_date=days_ago(2),
        dagrun_timeout=timedelta(minutes=60),
        tags=['ingest','transform','load'],
        params={'ejemplo':'ejemplo'},
) as dag:
        inicio = DummyOperator(
                task_id='inicio',
        )

        fin = DummyOperator(
                task_id='fin',
        )


        ingest = BashOperator(
                task_id='ingest',
                bash_command= '/usr/bin/sh /home/hadoop/scripts/ingest-informe-anac.sh ',
        )


        transform = BashOperator(
                task_id='transform',
                bash_command='ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/transform-informe-anac.py ',
        )



        inicio >> ingest >> transform >> fin
if __name__ == '__main__':
        dag.cli()
