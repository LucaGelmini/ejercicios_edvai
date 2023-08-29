CREATE TABLE IF NOT EXISTS payments
(
    VendorID int,
    tpep_pickup_datetetime date,
    payment_type int,
    total_amount float
)
STORED AS PARQUET;


LOAD DATA INPATH '/ingest/yellow_tripdata_2021-01.parquet' INTO TABLE payments;


