rm /home/hadoop/landing/yellow_tripdata_2021-01.parquet
hdfs dfs -rm /ingest/yellow_tripdata_2021-01.parquet
wget -P /home/hadoop/landing https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet

hdfs dfs -cp /home/hadoop/landing/yellow_tripdata_2021-01.parquet /ingest/ejercicio_6/congestion/yellow_tripdata_2021-01.parquet
hdfs dfs -put /home/hadoop/landing/yellow_tripdata_2021-01.parquet /ingest/ejercicio_6/distance/yellow_tripdata_2021-01.parquet
hdfs dfs -put /home/hadoop/landing/yellow_tripdata_2021-01.parquet /ingest/ejercicio_6/passengers/yellow_tripdata_2021-01.parquet
hdfs dfs -put /home/hadoop/landing/yellow_tripdata_2021-01.parquet /ingest/ejercicio_6/payments/yellow_tripdata_2021-01.parquet
hdfs dfs -put /home/hadoop/landing/yellow_tripdata_2021-01.parquet /ingest/ejercicio_6/tolls/yellow_tripdata_2021-01.parquet


hive -f ./crea_tablas.hql
