USE team14_projectdb;

DROP TABLE IF EXISTS q5_results;

CREATE TABLE q5_results (
    avg_carrier_delay_min      DOUBLE,
    avg_weather_delay_min      DOUBLE,
    avg_nas_delay_min          DOUBLE,
    avg_security_delay_min     DOUBLE,
    avg_late_aircraft_delay_min DOUBLE
)
STORED AS PARQUET;

INSERT INTO TABLE q5_results
SELECT
    ROUND(AVG(CAST(delay_due_carrier       AS DOUBLE)), 2) AS avg_carrier_delay_min,
    ROUND(AVG(CAST(delay_due_weather       AS DOUBLE)), 2) AS avg_weather_delay_min,
    ROUND(AVG(CAST(delay_due_nas           AS DOUBLE)), 2) AS avg_nas_delay_min,
    ROUND(AVG(CAST(delay_due_security      AS DOUBLE)), 2) AS avg_security_delay_min,
    ROUND(AVG(CAST(delay_due_late_aircraft AS DOUBLE)), 2) AS avg_late_aircraft_delay_min
FROM flights
WHERE CAST(arr_delay AS DOUBLE) > 0
  AND (
      delay_due_carrier       IS NOT NULL OR
      delay_due_weather       IS NOT NULL OR
      delay_due_nas           IS NOT NULL OR
      delay_due_security      IS NOT NULL OR
      delay_due_late_aircraft IS NOT NULL
  );

SELECT * FROM q5_results;
