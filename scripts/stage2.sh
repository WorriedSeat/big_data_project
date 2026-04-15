set -e

mkdir -p output

hive_password=$(cat secrets/.hive.pass | tr -d '\n\r')

BEELINE="beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team14 -p $hive_password --hiveconf hive.resultset.use.unique.column.names=false"

echo "=== Creating Hive database and tables ==="
$BEELINE -f sql/hive_db.hql 2>&1 | tee output/hive_results.txt
echo "Done. See output/hive_results.txt"

echo "=== Running Q1: Average arrival delay by airline ==="
$BEELINE -f sql/q1.hql 2>&1 | tee output/q1_hive.txt
echo "Done."

echo "=== Running Q2: Monthly flight volume and delays ==="
$BEELINE -f sql/q2.hql 2>&1 | tee output/q2_hive.txt
echo "Done."

echo "=== Running Q3: Top 10 busiest routes ==="
$BEELINE -f sql/q3.hql 2>&1 | tee output/q3_hive.txt
echo "Done."

echo "=== Running Q4: Cancellation rate by airline ==="
$BEELINE -f sql/q4.hql 2>&1 | tee output/q4_hive.txt
echo "Done."

echo "=== Running Q5: Average delay by cause ==="
$BEELINE -f sql/q5.hql 2>&1 | tee output/q5_hive.txt
echo "Done."

echo "=== Exporting results to CSV ==="
BEELINE_CSV="beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team14 -p $hive_password --outputformat=csv2 --showHeader=true --silent=true"
$BEELINE_CSV -e "SELECT * FROM team14_projectdb.q1_results;" > output/q1.csv
$BEELINE_CSV -e "SELECT * FROM team14_projectdb.q2_results;" > output/q2.csv
$BEELINE_CSV -e "SELECT * FROM team14_projectdb.q3_results;" > output/q3.csv
$BEELINE_CSV -e "SELECT * FROM team14_projectdb.q4_results;" > output/q4.csv
$BEELINE_CSV -e "SELECT * FROM team14_projectdb.q5_results;" > output/q5.csv
    
echo "=== Stage 2 finished ==="
