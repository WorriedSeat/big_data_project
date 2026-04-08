START TRANSACTION;

-- Drop existing table
DROP TABLE IF EXISTS flights CASCADE;
DROP TABLE IF EXISTS airlines CASCADE;
DROP TABLE IF EXISTS airports CASCADE;

-- Airlines
CREATE TABLE IF NOT EXISTS airlines (
    airline_code    VARCHAR(2)   PRIMARY KEY,
    airline_name    VARCHAR(100) NOT NULL,
    airline_dot     VARCHAR(100),
    dot_code        INTEGER      UNIQUE
);

-- Airports
CREATE TABLE IF NOT EXISTS airports (
    iata_code       VARCHAR(3)   PRIMARY KEY,
    city_name       VARCHAR(100) NOT NULL
);


-- Flights
CREATE TABLE IF NOT EXISTS flights (
    flight_id                   BIGSERIAL PRIMARY KEY,

    -- FKs
    airline_code                VARCHAR(2) NOT NULL,
    origin                      VARCHAR(3) NOT NULL,
    dest                        VARCHAR(3) NOT NULL,

    -- Flight info
    fl_number                   INTEGER NOT NULL,
    fl_date                     DATE NOT NULL,

    -- Time info
    crs_dep_time                TIME NOT NULL,
    dep_time                    VARCHAR(8),
    crs_arr_time                VARCHAR(8),
    arr_time                    VARCHAR(8),

    -- Delays and taxis
    dep_delay                   DECIMAL(10,2),
    arr_delay                   DECIMAL(10,2),
    taxi_out                    DECIMAL(6,2),
    wheels_off                  VARCHAR(8),
    wheels_on                   VARCHAR(8),
    taxi_in                     DECIMAL(6,2),

    -- Status
    cancelled                   BOOLEAN DEFAULT FALSE,
    cancellation_code           VARCHAR(5),
    diverted                    BOOLEAN DEFAULT FALSE,

    -- Time of the flight & distance
    crs_elapsed_time            DECIMAL(8,2),
    elapsed_time                DECIMAL(8,2),
    air_time                    DECIMAL(8,2),
    distance                    DECIMAL(10,2),   -- in km

    -- Delay's reasons
    delay_due_carrier           DECIMAL(10,2),
    delay_due_weather           DECIMAL(10,2),
    delay_due_nas               DECIMAL(10,2),
    delay_due_security          DECIMAL(10,2),
    delay_due_late_aircraft     DECIMAL(10,2),

    CONSTRAINT fk_flights_airline 
        FOREIGN KEY (airline_code) REFERENCES airlines(airline_code),

    CONSTRAINT fk_flights_origin 
        FOREIGN KEY (origin) REFERENCES airports(iata_code),

    CONSTRAINT fk_flights_dest 
        FOREIGN KEY (dest) REFERENCES airports(iata_code),

    -- Unique key
    CONSTRAINT uk_flight 
        UNIQUE (fl_date, airline_code, fl_number, origin, dest, crs_dep_time),
    CONSTRAINT chk_distance   CHECK (distance > 0)
);

-- Setting data format
ALTER DATABASE team14_projectdb SET datestyle TO iso, ymd;

COMMIT;
