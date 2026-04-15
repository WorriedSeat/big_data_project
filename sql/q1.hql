USE team14_projectdb;

DROP TABLE IF EXISTS q1_results;

CREATE TABLE q1_results (
    airline_name       STRING,
    avg_arr_delay_min  DOUBLE,
    total_flights      BIGINT
)
STORED AS PARQUET;

INSERT INTO TABLE q1_results
SELECT
    a.airline_name,
    ROUND(AVG(CAST(f.arr_delay AS DOUBLE)), 2) AS avg_arr_delay_min,
    COUNT(*)                                   AS total_flights
FROM flights f
JOIN airlines a ON f.airline_code = a.airline_code
WHERE f.cancelled = false
  AND f.arr_delay IS NOT NULL
GROUP BY a.airline_name
ORDER BY avg_arr_delay_min ASC;

SELECT * FROM q1_results;
