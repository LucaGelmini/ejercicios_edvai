DROP DATABASE IF EXISTS car_rental_db CASCADE;
CREATE DATABASE car_rental_db;

USE car_rental_db;

DROP TABLE IF EXISTS car_rental_analytics;
CREATE TABLE car_rental_analytics(
    fuelType STRING,
    rating INT,
    renterTripsTaken INT,
    reviewCount INT,
    city STRING,
    state_name STRING,
    owner_id INT,
    rate_daily INT,
    make STRING,
    model STRING,
    `year` INT
);