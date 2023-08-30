DROP TABLE IF EXISTS passengers
CREATE TABLE IF NOT EXISTS passengers
(
    tpep_pickup_datetetime,
    passenger_count,
    total_amount
)
STORED AS PARQUET;

LOAD DATA INPATH '/ingest/yellow_tripdata_2021-01.parquet' INTO TABLE passengers;