DROP TABLE IF EXISTS tolls
CREATE TABLE IF NOT EXISTS tolls
(
    tpep_pickup_datetetime,
    passenger_count,
    tolls_amount,
    total_amount
)
STORED AS PARQUET;
LOAD DATA INPATH '/ingest/yellow_tripdata_2021-01.parquet' INTO TABLE tolls;