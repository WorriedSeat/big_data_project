DROP DATABASE IF EXISTS team14_projectdb CASCADE;
CREATE DATABASE team14_projectdb
    LOCATION '/user/team14/project/hive/warehouse';

USE team14_projectdb;


CREATE EXTERNAL TABLE airlines (
    airline_code    STRING,
    airline_name    STRING,
    airline_dot     STRING,
    dot_code        INT
)
STORED AS PARQUET
LOCATION '/user/team14/project/warehouse/airlines';


CREATE EXTERNAL TABLE airports (
    iata_code   STRING,
    city_name   STRING
)
STORED AS PARQUET
LOCATION '/user/team14/project/warehouse/airports';


CREATE EXTERNAL TABLE flights (
    flight_id               BIGINT,
    airline_code            STRING,
    origin                  STRING,
    dest                    STRING,
    fl_number               INT,
    fl_date                 BIGINT,
    crs_dep_time            BIGINT,
    dep_time                BIGINT,
    crs_arr_time            BIGINT,
    arr_time                BIGINT,
    dep_delay               DECIMAL(10,2),
    arr_delay               DECIMAL(10,2),
    taxi_out                DECIMAL(6,2),
    wheels_off              BIGINT,
    wheels_on               BIGINT,
    taxi_in                 DECIMAL(6,2),
    cancelled               BOOLEAN,
    cancellation_code       STRING,
    diverted                BOOLEAN,
    crs_elapsed_time        DECIMAL(8,2),
    elapsed_time            DECIMAL(8,2),
    air_time                DECIMAL(8,2),
    distance                DECIMAL(10,2),
    delay_due_carrier       DECIMAL(10,2),
    delay_due_weather       DECIMAL(10,2),
    delay_due_nas           DECIMAL(10,2),
    delay_due_security      DECIMAL(10,2),
    delay_due_late_aircraft DECIMAL(10,2)
)
STORED AS PARQUET
LOCATION '/user/team14/project/warehouse/flights';


CREATE EXTERNAL TABLE flights_part (
    flight_id               BIGINT,
    airline_code            STRING,
    origin                  STRING,
    dest                    STRING,
    fl_number               INT,
    fl_date                 BIGINT,
    crs_dep_time            BIGINT,
    dep_time                BIGINT,
    crs_arr_time            BIGINT,
    arr_time                BIGINT,
    dep_delay               DECIMAL(10,2),
    arr_delay               DECIMAL(10,2),
    taxi_out                DECIMAL(6,2),
    wheels_off              BIGINT,
    wheels_on               BIGINT,
    taxi_in                 DECIMAL(6,2),
    cancelled               BOOLEAN,
    cancellation_code       STRING,
    diverted                BOOLEAN,
    crs_elapsed_time        DECIMAL(8,2),
    elapsed_time            DECIMAL(8,2),
    air_time                DECIMAL(8,2),
    distance                DECIMAL(10,2),
    delay_due_carrier       DECIMAL(10,2),
    delay_due_weather       DECIMAL(10,2),
    delay_due_nas           DECIMAL(10,2),
    delay_due_security      DECIMAL(10,2),
    delay_due_late_aircraft DECIMAL(10,2)
)
PARTITIONED BY (flight_year INT)
STORED AS PARQUET
LOCATION '/user/team14/project/hive/warehouse/flights_part'
TBLPROPERTIES ('parquet.compress'='SNAPPY');


SHOW TABLES;

SELECT COUNT(*) AS airlines_count FROM airlines;
SELECT COUNT(*) AS airports_count FROM airports;
SELECT COUNT(*) AS flights_count  FROM flights;
