
# Ejercicio 1 - Aviación civil
> La Administración Nacional de Aviación Civil necesita una serie de informes para elevar al ministerio de transporte acerca de los aterrizajes y despegues en todo el territorio Argentino, como puede ser: cuales aviones son los que más volaron, cuántos pasajeros volaron, ciudades de partidas y aterrizajes entre fechas determinadas, etc. Usted como data engineer deberá realizar un pipeline con esta información, automatizarlo y realizar los análisis de datos solicitados que permita responder las preguntas de negocio, y hacer sus recomendaciones con respecto al estado actual.

## Tareas
### Ingest
Ver `ingest-informe-anac.sh` que es ejecutado por el DAG de Airflow.
### Crear tablas en Hive
`docker cp edvai_hadoop:/home/hadoop/scripts/ <dirección de crea_tablas_informe-anac.hql>`
`hive -f /home/hadoop/scripts/crea_tablas_informe-anac.hql`
### Transformación
Ver `transform-informe-anac.py` que es ejecutado por el DAG de Airflow.
### Orquestación
El proceso completo se ejecuta desde Airflow, con el DAG `dag-informe-anac.py`
![DAG de Airflow](screenshots/DAG1.jpg)
### Análisis de datos
Esquema de tablas:
- Esquema de tabla vuelos
![DAG de Airflow](screenshots/esquema_aeropuertos.jpg)
- Esuema de tabla aeropuertos
![DAG de Airflow](screenshots/esquema_vuelos.jpg)

6. Determinar la cantidad de vuelos entre las fechas 01/12/2021 y 31/01/2022. Mostrar consulta y Resultado de la query
   ![DAG de Airflow](screenshots/queries_6.jpg)
7. Cantidad de pasajeros que viajaron en Aerolíneas Argentinas entre el 01/01/2021 y 30/06/2022. Mostrar consulta y Resultado de la query
   ![DAG de Airflow](screenshots/queries_7.jpg)
8. Mostrar fecha, hora, código aeropuerto salida, ciudad de salida, código de aeropuerto de arribo, ciudad de arribo, y cantidad de pasajeros de cada vuelo, entre el 01/01/2022 y el 30/06/2022 ordenados por fecha de manera descendiente. Mostrar consulta y Resultado de la query
   ![DAG de Airflow](screenshots/queries_8.jpg)
9.  Cuales son las 10 aerolíneas que más pasajeros llevaron entre el 01/01/2021 y el 30/06/2022 exceptuando aquellas aerolíneas que no tengan nombre. Mostrar consulta y Visualización
    ![DAG de Airflow](screenshots/queries_9.jpg)
10.  Cuales son las 10 aeronaves más utilizadas entre el 01/01/2021 y el 30/06/22 que despegaron desde la Ciudad autónoma de Buenos Aires o de Buenos Aires, exceptuando aquellas aeronaves que no cuentan con nombre. Mostrar consulta y Visualización
    ![DAG de Airflow](screenshots/queries_10.jpg)