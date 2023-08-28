CREATE TABLE IF NOT EXISTS congestion 
(
    tpep_pickup_datetetime date,
    passenger_count int,
    congestion_surcharge float,
    total_amount float
)
STORED AS PARQUET;

LOAD DATA INPATH '/ingest/yellow_tripdata_2021-01.parquet' INTO TABLE congestion;