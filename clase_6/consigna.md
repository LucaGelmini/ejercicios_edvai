# Práctica Hive
**Consigna:** Por cada ejercicio, escribir el código y agregar una captura de pantalla del resultado
obtenido.

- Diccionario de datos: [yellow trip data](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)
1. En Hive, crear las siguientes tablas (internas) en la base de datos tripdata en hive:
    a. payments(VendorID, tpep_pickup_datetetime, payment_type, total_amount)
    b. passengers(tpep_pickup_datetetime, passenger_count, total_amount)
    c. tolls (tpep_pickup_datetetime, passenger_count, tolls_amount, total_amount)
    d. congestion (tpep_pickup_datetetime, passenger_count, congestion_surcharge, total_amount)
    e. distance (tpep_pickup_datetetime, passenger_count, trip_distance, total_amount)
2. En Hive, hacer un ‘describe’ de las tablas passengers y distance.
3. Hacer ingest del file: Yellow_tripodata_2021-01.csv
    https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv
    Para los siguientes ejercicios, debes usar PySpark (obligatorio). Si deseas practicar más, también puedes repetir los mismos en SQL (opcional)
4. (Opcional SQL) Generar una vista
5. Insertar en la tabla payments (VendorID, tpep_pickup_datetetime, payment_type, total_amount) Solamente los pagos con tarjeta de crédito.
Table
6. Insertar en la tabla passengers (tpep_pickup_datetetime, passenger_count, total_amount) los registros cuya cantidad de pasajeros sea mayor a 2 y el total del viaje cueste más de 8 dólares.
7. Insertar en la tabla tolls (tpep_pickup_datetetime, passenger_count, tolls_amount, total_amount) los registros que tengan pago de peajes mayores a 0.1 y cantidad de pasajeros mayores a 1.
8. Insertar en la tabla congestion (tpep_pickup_datetetime, passenger_count, congestion_surcharge, total_amount) los registros que hayan tenido congestión en los viajes en la fecha 2021-01-18.
9. Insertar en la tabla distance (tpep_pickup_datetetime, passenger_count, trip_distance, total_amount) los registros de la fecha 2020-12-31 que hayan tenido solamente un pasajero (passenger_count = 1) y hayan recorrido más de 15 millas (trip_distance).