password=$(cat secrets/.psql.pass | tr -d '\n\r')

hdfs dfs -rm -r -f /user/team14/project/warehouse || true
hdfs dfs -mkdir -p /user/team14/project/warehouse

echo "Loading data in hdfs (Parquet + Snappy)..."

sqoop import-all-tables \
    --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team14_projectdb \
    --username team14 \
    --password "$password" \
    --warehouse-dir /user/team14/project/warehouse \
    --as-parquetfile \
    --compression-codec=snappy \
    --m 1

echo "Loaded everything on HDFS"

echo ""
echo "Results in HDFS:"
hdfs dfs -ls /user/team14/project/warehouse
