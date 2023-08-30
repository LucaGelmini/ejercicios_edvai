DROP TABLE IF EXISTS  payments
CREATE TABLE IF NOT EXISTS payments
(
    VendorID,
    tpep_pickup_datetetime,
    payment_type,
    total_amount
)
STORED AS PARQUET;


LOAD DATA INPATH '/ingest/yellow_tripdata_2021-01.parquet' INTO TABLE payments;


