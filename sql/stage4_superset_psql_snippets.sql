-- Copy into Superset SQL Lab for "data description" charts (PostgreSQL team14_projectdb).

SELECT schemaname, relname AS table_name, n_live_tup AS approx_row_count
FROM pg_stat_user_tables
WHERE relname IN ('airlines', 'airports', 'flights')
ORDER BY relname;

SELECT table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name IN ('airlines', 'airports', 'flights')
ORDER BY table_name, ordinal_position;

SELECT * FROM airlines LIMIT 50;
SELECT * FROM airports LIMIT 50;
SELECT * FROM flights LIMIT 50;
