DROP TABLE IF EXISTS distance;
CREATE TABLE IF NOT EXISTS distance 
(
    tpep_pickup_datetetime,
    passenger_count,
    trip_distance,
    total_amount
)
STORED AS PARQUET;

LOAD DATA INPATH '/ingest/yellow_tripdata_2021-01.parquet' INTO TABLE distance;