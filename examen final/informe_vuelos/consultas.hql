/*
 * 6. Determinar la cantidad de vuelos entre las fechas 01/12/2021 y 31/01/2022. Mostrar consulta y Resultado de la query
 */
SELECT COUNT(*) as cuenta  FROM vuelos v WHERE v.datetime BETWEEN '2021-12-01' AND '2022-01-31';

-- 7. Cantidad de pasajeros que viajaron en Aerolíneas Argentinas entre el 01/01/2021 y 30/06/2022. Mostrar consulta y Resultado de la query
SELECT sum(pasajeros) as Total_pasajeros 
FROM vuelos v
WHERE v.datetime BETWEEN '2021-01-01' AND '2022-06-30';

/*
 * 8. Mostrar fecha, hora, código aeropuerto salida, ciudad de salida,
 *  código de aeropuerto de arribo, ciudad de arribo, y cantidad de 
 * pasajeros de cada vuelo, entre el 01/01/2022 y el 30/06/2022 ordenados 
 * por fecha de manera descendiente. Mostrar consulta y Resultado de la query
 */ 
SELECT datetime as diaHora, v.fecha fecha, v.hora_utc as hora, v.aeropuerto as aero_salida, v.`origen_/_destino` as aero_arribo, a.`ref` as ciudad_arribo, v.pasajeros as pasajeros 
FROM vuelos v 
join aeropuertos a on v.`origen_/_destino` = a.`local` 
WHERE 
	(v.datetime BETWEEN '2022-01-01' AND '2022-06-30') AND 
	v.tipo_de_movimiento = 'Despegue'
ORDER by diaHora DESC;


/*
 * 9. Cuales son las 10 aerolíneas que más pasajeros llevaron entre el 01/01/2021 y el 30/06/2022
 * exceptuando aquellas aerolíneas que no tengan nombre. Mostrar consulta y Visualización
 */
SELECT aerolinea_nombre,SUM(pasajeros) as total_pasajeros  
FROM vuelos v
WHERE (v.datetime BETWEEN '2021-01-01' AND '2022-06-30') AND 
	(aerolinea_nombre not like 0)
GROUP by aerolinea_nombre
ORDER by total_pasajeros DESC 
LIMIT 10;

/*
 * 10. Cuales son las 10 aeronaves más utilizadas entre el 01/01/2021 y el 30/06/22 
 * que despegaron desde la Ciudad autónoma de Buenos Aires o de Buenos Aires, 
 * exceptuando aquellas aeronaves que no cuentan con nombre. Mostrar consulta y Visualización
 */

SELECT v.aeronave,COUNT(v.aeronave)as total_vuelos 
from vuelos v
join aeropuertos a on v.`origen_/_destino` = a.`local` 
WHERE (v.datetime BETWEEN '2021-01-01' AND '2022-06-30') AND 
	(aeronave  not like 0) AND 
	(tipo_de_movimiento like 'Despegue') AND 
	(LOWER(a.provincia)  LIKE '%buenos aires%') AND 
	(aeronave not like 0)
GROUP by v.aeronave
order by total_vuelos DESC
LIMIT 10;
