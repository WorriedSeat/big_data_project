USE team14_projectdb;

DROP TABLE IF EXISTS q2_results;

CREATE TABLE q2_results (
    year_month         STRING,
    flight_count       BIGINT,
    avg_dep_delay_min  DOUBLE,
    avg_arr_delay_min  DOUBLE
)
STORED AS PARQUET;

INSERT INTO TABLE q2_results
SELECT
    DATE_FORMAT(FROM_UNIXTIME(CAST(fl_date AS BIGINT) / 1000), 'yyyy-MM') AS year_month,
    COUNT(*)                                                                AS flight_count,
    ROUND(AVG(CAST(dep_delay AS DOUBLE)), 2)                               AS avg_dep_delay_min,
    ROUND(AVG(CAST(arr_delay AS DOUBLE)), 2)                               AS avg_arr_delay_min
FROM flights
WHERE cancelled = false
GROUP BY DATE_FORMAT(FROM_UNIXTIME(CAST(fl_date AS BIGINT) / 1000), 'yyyy-MM')
ORDER BY year_month;

SELECT * FROM q2_results;
