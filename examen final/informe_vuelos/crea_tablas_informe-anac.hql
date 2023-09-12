DROP DATABASE IF EXISTS informe_anac CASCADE;
CREATE DATABASE informe_anac;
USE informe_anac;
    CREATE TABLE vuelos (
        Fecha STRING,
        Hora_UTC STRING,
        Clase_de_Vuelo STRING,
        Tipo_de_Movimiento STRING, 
        Aeropuerto STRING,
        `Origen_/_Destino` STRING, 
        Aerolinea_Nombre STRING,
        Aeronave STRING,
        Pasajeros INT,
        `datetime` TIMESTAMP
    );
    CREATE TABLE aeropuertos (
        `local` STRING,
        oaci STRING,
        iata STRING,
        tipo STRING,
        denominacion STRING,
        coordenadas STRING,
        latitud STRING,
        longitud STRING,
        elev STRING,
        uom_elev FLOAT,
        ref STRING,
        distancia_ref FLOAT,
        direccion_ref STRING,
        condicion STRING,
        control STRING,
        region STRING,
        uso STRING,
        trafico STRING,
        sna STRING,
        concesionado STRING,
        provincia STRING);