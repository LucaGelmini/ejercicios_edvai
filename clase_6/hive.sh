rm /home/hadoop/landing/yellow_tripdata_2021-01.parquet
hdfs dfs -rm /ingest/yellow_tripdata_2021-01.parquet

wget -P /home/hadoop/landing https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet
hdfs dfs -put /home/hadoop/landing/yellow_tripdata_2021-01.parquet /ingest/yellow_tripdata_2021-01.parquet
hive -f ./crea_congestion.hql

wget -P /home/hadoop/landing https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet
hdfs dfs -put /home/hadoop/landing/yellow_tripdata_2021-01.parquet /ingest/yellow_tripdata_2021-01.parquet
hive -f ./crea_distance.hql

wget -P /home/hadoop/landing https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet
hdfs dfs -put /home/hadoop/landing/yellow_tripdata_2021-01.parquet /ingest/yellow_tripdata_2021-01.parquet
hive -f ./crea_passengers.hql

wget -P /home/hadoop/landing https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet
hdfs dfs -put /home/hadoop/landing/yellow_tripdata_2021-01.parquet /ingest/yellow_tripdata_2021-01.parquet
hive -f ./crea_payments.hql

wget -P /home/hadoop/landing https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet
hdfs dfs -put /home/hadoop/landing/yellow_tripdata_2021-01.parquet /ingest/yellow_tripdata_2021-01.parquet
hive -f ./crea_tolls.hql

wget -P /home/hadoop/landing https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet
hdfs dfs -put /home/hadoop/landing/yellow_tripdata_2021-01.parquet /ingest/yellow_tripdata_2021-01.parquet
hive -f ./describe.hql
