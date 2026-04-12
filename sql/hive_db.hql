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
    dep_delay               STRING,
    arr_delay               STRING,
    taxi_out                STRING,
    wheels_off              BIGINT,
    wheels_on               BIGINT,
    taxi_in                 STRING,
    cancelled               BOOLEAN,
    cancellation_code       STRING,
    diverted                BOOLEAN,
    crs_elapsed_time        STRING,
    elapsed_time            STRING,
    air_time                STRING,
    distance                STRING,
    delay_due_carrier       STRING,
    delay_due_weather       STRING,
    delay_due_nas           STRING,
    delay_due_security      STRING,
    delay_due_late_aircraft STRING
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
    dep_delay               STRING,
    arr_delay               STRING,
    taxi_out                STRING,
    wheels_off              BIGINT,
    wheels_on               BIGINT,
    taxi_in                 STRING,
    cancelled               BOOLEAN,
    cancellation_code       STRING,
    diverted                BOOLEAN,
    crs_elapsed_time        STRING,
    elapsed_time            STRING,
    air_time                STRING,
    distance                STRING,
    delay_due_carrier       STRING,
    delay_due_weather       STRING,
    delay_due_nas           STRING,
    delay_due_security      STRING,
    delay_due_late_aircraft STRING
)
PARTITIONED BY (flight_year INT)
STORED AS PARQUET
LOCATION '/user/team14/project/hive/warehouse/flights_part'
TBLPROPERTIES ('parquet.compress'='SNAPPY');


SHOW TABLES;

SELECT COUNT(*) AS airlines_count FROM airlines;
SELECT COUNT(*) AS airports_count FROM airports;
SELECT COUNT(*) AS flights_count  FROM flights;
