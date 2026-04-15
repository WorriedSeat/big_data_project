USE team14_projectdb;

DROP TABLE IF EXISTS q3_results;

CREATE TABLE q3_results (
    origin            STRING,
    origin_city       STRING,
    dest              STRING,
    dest_city         STRING,
    flight_count      BIGINT,
    avg_arr_delay_min DOUBLE
)
STORED AS PARQUET;

INSERT INTO TABLE q3_results
SELECT
    f.origin,
    ap1.city_name                                    AS origin_city,
    f.dest,
    ap2.city_name                                    AS dest_city,
    COUNT(*)                                         AS flight_count,
    ROUND(AVG(CAST(f.arr_delay AS DOUBLE)), 2)       AS avg_arr_delay_min
FROM flights f
JOIN airports ap1 ON f.origin = ap1.iata_code
JOIN airports ap2 ON f.dest   = ap2.iata_code
GROUP BY f.origin, ap1.city_name, f.dest, ap2.city_name
ORDER BY flight_count DESC
LIMIT 10;

SELECT * FROM q3_results;
