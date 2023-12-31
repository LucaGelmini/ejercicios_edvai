DROP DATABASE IF EXISTS tripdata_ejercicio CASCADE;
CREATE DATABASE IF NOT EXISTS tripdata_ejercicio;

USE tripdata_ejercicio;

DROP TABLE IF EXISTS congestion;

CREATE TABLE IF NOT EXISTS congestion 
(
    tpep_pickup_datetime STRING,
    passenger_count INT,
    congestion_surcharge DOUBLE,
    total_amount DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '\"'
)
STORED AS TEXTFILE;

LOAD DATA INPATH '/ingest/ejercicio_6/congestion/yellow_tripdata_2021-01.csv' INTO TABLE congestion;

DROP TABLE IF EXISTS distance;
CREATE TABLE IF NOT EXISTS distance 
(
    tpep_pickup_datetime STRING,
    passenger_count INT,
    trip_distance DOUBLE,
    total_amount DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '\"'
)
STORED AS TEXTFILE;

LOAD DATA INPATH '/ingest/ejercicio_6/distance/yellow_tripdata_2021-01.csv' INTO TABLE distance;

DROP TABLE IF EXISTS passengers;

CREATE TABLE IF NOT EXISTS passengers
(
    tpep_pickup_datetime STRING,
    passenger_count INT,
    total_amount DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '\"'
)
STORED AS TEXTFILE;

LOAD DATA INPATH '/ingest/ejercicio_6/passengers/yellow_tripdata_2021-01.csv' INTO TABLE passengers;

DROP TABLE IF EXISTS  payments;
CREATE TABLE IF NOT EXISTS payments
(
    VendorID INT,
    tpep_pickup_datetime STRING,
    payment_type INT,
    total_amount DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '\"'
)
STORED AS TEXTFILE;

LOAD DATA INPATH '/ingest/ejercicio_6/payments/yellow_tripdata_2021-01.csv' INTO TABLE payments;

DROP TABLE IF EXISTS tolls;
CREATE TABLE IF NOT EXISTS tolls
(
    tpep_pickup_datetime STRING,
    passenger_count INT,
    tolls_amount DOUBLE,
    total_amount DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '\"'
)
STORED AS TEXTFILE;

LOAD DATA INPATH '/ingest/ejercicio_6/tolls/yellow_tripdata_2021-01.csv' INTO TABLE tolls;

DESCRIBE FORMATTED passengers;
DESCRIBE FORMATTED distance;
