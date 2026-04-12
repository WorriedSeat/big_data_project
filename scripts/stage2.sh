set -e

hive_password=$(cat secrets/.hive.pass | tr -d '\n\r')

echo "Creating Hive database and tables:"
beeline \
    -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 \
    -n team14 \
    -p "$hive_password" \
    --hiveconf hive.resultset.use.unique.column.names=false \
    -f sql/hive_db.hql \
    2>&1 | tee output/hive_results.txt

echo "Hive tables created. See output/hive_results.txt"


echo "Running EDA with PySpark:"
spark-submit \
    --master yarn \
    --jars /shared/postgresql-42.6.1.jar \
    scripts/eda.py \
    2>&1 | tee output/eda_results.txt

echo "EDA finished. See output/eda_results.txt"

echo "Stage 2 finished"
