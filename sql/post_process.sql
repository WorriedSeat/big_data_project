-- Creating new table with correct types
CREATE TABLE flights_new AS
SELECT
    flight_id,
    airline_code,
    origin,
    dest,
    fl_number,
    fl_date,

    crs_dep_time::TIME AS crs_dep_time,
    NULLIF(dep_time, '')::TIME         AS dep_time,
    NULLIF(crs_arr_time, '')::TIME     AS crs_arr_time,
    NULLIF(arr_time, '')::TIME         AS arr_time,

    dep_delay,
    arr_delay,
    taxi_out,

    NULLIF(wheels_off, '')::TIME       AS wheels_off,
    NULLIF(wheels_on, '')::TIME        AS wheels_on,

    taxi_in,
    cancelled,
    NULLIF(cancellation_code, '')::VARCHAR(1)       AS cancellation_code,
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

FROM flights;

-- Removing old table and renaming new one
DROP TABLE flights;
ALTER TABLE flights_new RENAME TO flights;

-- Adding constraints and indexes
ALTER TABLE flights 
    ADD CONSTRAINT fk_flights_airline FOREIGN KEY (airline_code) REFERENCES airlines(airline_code),
    ADD CONSTRAINT fk_flights_origin  FOREIGN KEY (origin)      REFERENCES airports(iata_code),
    ADD CONSTRAINT fk_flights_dest    FOREIGN KEY (dest)        REFERENCES airports(iata_code),
    ADD CONSTRAINT uk_flight UNIQUE (fl_date, airline_code, fl_number, origin, dest, crs_dep_time),
    ADD CONSTRAINT chk_distance CHECK (distance > 0);
