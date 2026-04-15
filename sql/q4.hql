USE team14_projectdb;

DROP TABLE IF EXISTS q4_results;

CREATE TABLE q4_results (
    airline_name          STRING,
    total_flights         BIGINT,
    cancelled_count       BIGINT,
    cancellation_rate_pct DOUBLE
)
STORED AS PARQUET;

INSERT INTO TABLE q4_results
SELECT
    a.airline_name,
    COUNT(*)                                                            AS total_flights,
    SUM(CASE WHEN f.cancelled = true THEN 1 ELSE 0 END)               AS cancelled_count,
    ROUND(
        100.0 * SUM(CASE WHEN f.cancelled = true THEN 1 ELSE 0 END)
              / COUNT(*), 2)                                            AS cancellation_rate_pct
FROM flights f
JOIN airlines a ON f.airline_code = a.airline_code
GROUP BY a.airline_name
ORDER BY cancellation_rate_pct DESC;

SELECT * FROM q4_results;
