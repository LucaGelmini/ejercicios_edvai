rm /home/hadoop/landing/yellow_tripdata_2021-01.parquet
hdfs dfs -rm /ingest/yellow_tripdata_2021-01.parquet
wget -P /home/hadoop/landing https://github.com/LucaGelmini/datasets/blob/main/yellow_tripdata_2021-01.csv

hdfs dfs -mkdir /ingest/ejercicio_6
hdfs dfs -mkdir /ingest/ejercicio_6/congestion
hdfs dfs -mkdir /ingest/ejercicio_6/distance
hdfs dfs -mkdir /ingest/ejercicio_6/passengers
hdfs dfs -mkdir /ingest/ejercicio_6/payments
hdfs dfs -mkdir /ingest/ejercicio_6/tolls

hdfs dfs -put /home/hadoop/landing/yellow_tripdata_2021-01.csv /ingest/ejercicio_6/congestion/yellow_tripdata_2021-01.csv
hdfs dfs -put /home/hadoop/landing/yellow_tripdata_2021-01.csv /ingest/ejercicio_6/distance/yellow_tripdata_2021-01.csv
hdfs dfs -put /home/hadoop/landing/yellow_tripdata_2021-01.csv /ingest/ejercicio_6/passengers/yellow_tripdata_2021-01.csv
hdfs dfs -put /home/hadoop/landing/yellow_tripdata_2021-01.csv /ingest/ejercicio_6/payments/yellow_tripdata_2021-01.csv
hdfs dfs -put /home/hadoop/landing/yellow_tripdata_2021-01.csv /ingest/ejercicio_6/tolls/yellow_tripdata_2021-01.csv
rm /home/hadoop/landing/yellow_tripdata_2021-01.csv


hive -f ./crea_tablas.hql
hdfs dfs -rm -r /ingest/ejercicio_6/


