-- Loading Airlines 
COPY airlines (airline_code, airline_name, airline_dot, dot_code)
FROM STDIN WITH CSV HEADER DELIMITER ',' NULL AS 'null';

-- Loading Airports
COPY airports (iata_code, city_name)
FROM STDIN WITH CSV HEADER DELIMITER ',' NULL AS 'null';

-- Loading Flights 
COPY flights (
    airline_code,
    origin,
    dest,
    fl_number,
    fl_date,
    crs_dep_time,
    dep_time,
    crs_arr_time,
    arr_time,
    dep_delay,
    arr_delay,
    taxi_out,
    wheels_off,
    wheels_on,
    taxi_in,
    cancelled,
    cancellation_code,
    diverted,
    crs_elapsed_time,
    elapsed_time,
    air_time,
    distance,
    delay_due_carrier,
    delay_due_weather,
    delay_due_nas,
    delay_due_security,
    delay_due_late_aircraft
)
FROM STDIN WITH CSV HEADER DELIMITER ',' NULL 'None';