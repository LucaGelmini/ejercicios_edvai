CREATE TABLE IF NOT EXISTS payments
(
    VendorID int,
    tpep_pickup_datetetime date,
    payment_type int,
    total_amount float
)
STORED AS PARQUET;
LOAD DATA INPATH '/ingest/yellow_tripdata_2021-01.parquet' INTO TABLE payments;

CREATE TABLE IF NOT EXISTS passengers
(
    tpep_pickup_datetetime date,
    passenger_count int,
    total_amount float
)
STORED AS PARQUET;
LOAD DATA INPATH '/ingest/yellow_tripdata_2021-01.parquet' INTO TABLE passengers;

CREATE TABLE IF NOT EXISTS tolls
(
    tpep_pickup_datetetime date,
    passenger_count int,
    tolls_amount float,
    total_amount float
)
STORED AS PARQUET;
LOAD DATA INPATH '/ingest/yellow_tripdata_2021-01.parquet' INTO TABLE tolls;

CREATE TABLE IF NOT EXISTS congestion 
(
    tpep_pickup_datetetime date,
    passenger_count int,
    congestion_surcharge float,
    total_amount float
)
STORED AS PARQUET;
LOAD DATA INPATH '/ingest/yellow_tripdata_2021-01.parquet' INTO TABLE congestion ;

CREATE TABLE IF NOT EXISTS distance 
(
    tpep_pickup_datetetime date,
    passenger_count int,
    trip_distance float,
    total_amount float
)
STORED AS PARQUET;
LOAD DATA INPATH '/ingest/yellow_tripdata_2021-01.parquet' INTO TABLE distance ;


DESCRIBE FORMATTED passengers;
DESCRIBE FORMATTED distance;