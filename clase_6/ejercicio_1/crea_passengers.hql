CREATE TABLE IF NOT EXISTS passengers
(
    tpep_pickup_datetetime date,
    passenger_count int,
    total_amount float
)
STORED AS PARQUET;

LOAD DATA INPATH '/ingest/yellow_tripdata_2021-01.parquet' INTO TABLE passengers;