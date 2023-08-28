wget -P /home/hadoop/landing https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet


hdfs dfs -rm /ingest/yellow_tripdata_2021-01.parquet
hdfs dfs -put ./yellow_tripdata_2021-01.parquet /ingest

hive -f ./crea_tablas.hql