import os
from pyspark.sql import SparkSession

WAREHOUSE      = "/user/team14/project/hive/warehouse"
HIVE_METASTORE = "thrift://hadoop-02.uni.innopolis.ru:9883"
PG_URL         = "jdbc:postgresql://hadoop-04.uni.innopolis.ru/team14_projectdb"
PG_USER        = "team14"
PG_JAR         = "/shared/postgresql-42.6.1.jar"


def _get_pg_password():
    with open(os.path.join("secrets", ".psql.pass")) as f:
        return f.read().strip()


def _create_spark():
    password = _get_pg_password()
    return (
        SparkSession.builder
        .master("yarn")
        .appName("Stage2-EDA")
        .config("spark.sql.catalogImplementation", "hive")
        .config("hive.metastore.uris", HIVE_METASTORE)
        .config("spark.sql.warehouse.dir", WAREHOUSE)
        .config("spark.driver.extraClassPath", PG_JAR)
        .config("spark.jars", PG_JAR)
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .enableHiveSupport()
        .getOrCreate()
    )


def _run_query(spark, name, sql, description):

    df = spark.sql(sql)
    df.show(25, truncate=False)

    pdf = df.toPandas()
    out_path = os.path.join("output", f"{name}.csv")
    pdf.to_csv(out_path, index=False)
    print(f"  -> Saved CSV: {out_path}")

    password = _get_pg_password()
    (df.write
       .format("jdbc")
       .option("url", PG_URL)
       .option("dbtable", f"{name}_results")
       .option("user", PG_USER)
       .option("password", password)
       .option("driver", "org.postgresql.Driver")
       .mode("overwrite")
       .save())
    print(f"  -> Saved to PostgreSQL table: {name}_results")

    return df


def _populate_flights_part(spark):
    """Fill flights_part (partitioned by flight_year) from the base flights table."""
    print("\nPopulating flights_part (partitioned by year)...")
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    spark.sql("""
        INSERT OVERWRITE TABLE team14_projectdb.flights_part
        PARTITION (flight_year)
        SELECT
            flight_id,
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
            delay_due_late_aircraft,
            CAST(SUBSTR(fl_date, 1, 4) AS INT) AS flight_year
        FROM team14_projectdb.flights
    """)
    print("flights_part populated.")
    spark.sql("SHOW PARTITIONS team14_projectdb.flights_part").show()


def main():
    os.makedirs("output", exist_ok=True)
    spark = _create_spark()

    _populate_flights_part(spark)

    # Q1: Average arrival delay by airline (ascending = most punctual)
    _run_query(spark, "q1", """
        SELECT
            a.airline_name,
            ROUND(AVG(f.arr_delay), 2)  AS avg_arr_delay_min,
            COUNT(*)                    AS total_flights
        FROM team14_projectdb.flights  f
        JOIN team14_projectdb.airlines a ON f.airline_code = a.airline_code
        WHERE f.cancelled = false
          AND f.arr_delay IS NOT NULL
        GROUP BY a.airline_name
        ORDER BY avg_arr_delay_min ASC
    """, "Average arrival delay by airline")

    # Q2: Monthly flight volume and average delays (seasonal trends)
    _run_query(spark, "q2", """
        SELECT
            SUBSTR(fl_date, 1, 7)           AS year_month,
            COUNT(*)                        AS flight_count,
            ROUND(AVG(dep_delay), 2)        AS avg_dep_delay_min,
            ROUND(AVG(arr_delay), 2)        AS avg_arr_delay_min
        FROM team14_projectdb.flights
        WHERE cancelled = false
        GROUP BY SUBSTR(fl_date, 1, 7)
        ORDER BY year_month
    """, "Monthly flight volume and average delays")

    # Q3: Top 10 busiest routes by number of flights
    _run_query(spark, "q3", """
        SELECT
            f.origin,
            ap1.city_name               AS origin_city,
            f.dest,
            ap2.city_name               AS dest_city,
            COUNT(*)                    AS flight_count,
            ROUND(AVG(f.arr_delay), 2)  AS avg_arr_delay_min
        FROM team14_projectdb.flights  f
        JOIN team14_projectdb.airports ap1 ON f.origin = ap1.iata_code
        JOIN team14_projectdb.airports ap2 ON f.dest   = ap2.iata_code
        GROUP BY f.origin, ap1.city_name, f.dest, ap2.city_name
        ORDER BY flight_count DESC
        LIMIT 10
    """, "Top 10 busiest routes")

    # Q4: Cancellation rate by airline (%)
    _run_query(spark, "q4", """
        SELECT
            a.airline_name,
            COUNT(*)                                                          AS total_flights,
            SUM(CASE WHEN f.cancelled = true THEN 1 ELSE 0 END)             AS cancelled_count,
            ROUND(
                100.0 * SUM(CASE WHEN f.cancelled = true THEN 1 ELSE 0 END)
                      / COUNT(*), 2)                                         AS cancellation_rate_pct
        FROM team14_projectdb.flights  f
        JOIN team14_projectdb.airlines a ON f.airline_code = a.airline_code
        GROUP BY a.airline_name
        ORDER BY cancellation_rate_pct DESC
    """, "Cancellation rate by airline (%)")

    # Q5: Average delay contribution by cause (delayed flights only)
    _run_query(spark, "q5", """
        SELECT
            ROUND(AVG(delay_due_carrier),       2) AS avg_carrier_delay_min,
            ROUND(AVG(delay_due_weather),       2) AS avg_weather_delay_min,
            ROUND(AVG(delay_due_nas),           2) AS avg_nas_delay_min,
            ROUND(AVG(delay_due_security),      2) AS avg_security_delay_min,
            ROUND(AVG(delay_due_late_aircraft), 2) AS avg_late_aircraft_delay_min
        FROM team14_projectdb.flights
        WHERE arr_delay > 0
          AND (
              delay_due_carrier       IS NOT NULL OR
              delay_due_weather       IS NOT NULL OR
              delay_due_nas           IS NOT NULL OR
              delay_due_security      IS NOT NULL OR
              delay_due_late_aircraft IS NOT NULL
          )
    """, "Average delay breakdown by cause (minutes, delayed flights only)")

    print("  Stage 2 EDA complete. Results saved to output/")
    spark.stop()


if __name__ == "__main__":
    main()
