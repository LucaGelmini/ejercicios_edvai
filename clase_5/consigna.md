1. En el container de Nifi, crear un .sh que permita descargar el archivo de
   https://data-engineer-edvai.s3.amazonaws.com/yellow_tripdata_2021-01.parquet y lo
   guarde en /home/nifi/ingest.
   Ejecutarlo.
2. Por medio de la interfaz gráfica de Nifi, crear un job que tenga dos procesos.
   a) GetFile para obtener el archivo del punto 1 (/home/nifi/ingest)
   b) putHDFS para ingestarlo a HDFS (directorio nifi)
3. Con el archivo ya ingestado en HDFS/nifi, escribir las consultas y agregar captura de
   pantalla del resultado. Para los ejercicios puedes usar SQL mediante la creación de una
   vista llamada yellow_tripdata.
   También debes chequear el diccionario de datos por cualquier duda que tengas
   respecto a las columnas del archivo
   https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
   1. Mostrar los resultados siguientes
      a. VendorId Integer
      b. Tpep_pickup_datetime date
      c. Total_amount double
      d. Donde el total (total_amount sea menor a 10 dólares).
   2. Mostrar los 10 días que más se recaudó dinero (tpep_pickup_datetime, total
      amount)
   3. Mostrar los 10 viajes que menos dinero recaudó en viajes mayores a 10 millas
      (trip_distance, total_amount)
   4. Mostrar los viajes de más de dos pasajeros que hayan pagado con tarjeta de
      crédito (mostrar solo las columnas trip_distance y tpep_pickup_datetime)
   5. Mostrar los 7 viajes con mayor propina en distancias mayores a 10 millas (mostrar
      campos tpep_pickup_datetime, trip_distance, passenger_count, tip_amount)
   6. Mostrar para cada uno de los valores de RateCodeID, el monto total y el monto
      promedio. Excluir los viajes en donde RateCodeID es ‘Group Ride’
