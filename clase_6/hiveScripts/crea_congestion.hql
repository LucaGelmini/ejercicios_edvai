DROP TABLE IF EXISTS congestion
CREATE TABLE IF NOT EXISTS congestion 
(
    tpep_pickup_datetetime,
    passenger_count,
    congestion_surcharge,
    total_amount
)
STORED AS PARQUET;

LOAD DATA INPATH '/ingest/yellow_tripdata_2021-01.parquet' INTO TABLE congestion;