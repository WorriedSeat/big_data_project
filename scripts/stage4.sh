#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

mkdir -p output

PASS_FILE="${ROOT}/secrets/.hive.pass"
if [[ ! -f "${PASS_FILE}" ]]; then
  echo "stage4: missing ${PASS_FILE}" >&2
  exit 1
fi

hive_password=$(tr -d '\n\r' <"${PASS_FILE}")
BEELINE=(
  beeline -u "jdbc:hive2://hadoop-03.uni.innopolis.ru:10001"
  -n team14 -p "${hive_password}"
  --hiveconf hive.resultset.use.unique.column.names=false
)

HDFS_PREFIX="${STAGE4_HDFS_PREFIX:-/user/team14/project}"

hdfs dfs -mkdir -p "${HDFS_PREFIX}/stage4_catalog/ml_features"
hdfs dfs -mkdir -p "${HDFS_PREFIX}/stage4_catalog/ml_hyper"
hdfs dfs -put -f "${ROOT}/sql/data/stage4_ml_features.csv" "${HDFS_PREFIX}/stage4_catalog/ml_features/"
hdfs dfs -put -f "${ROOT}/sql/data/stage4_ml_hyper.csv" "${HDFS_PREFIX}/stage4_catalog/ml_hyper/"

"${BEELINE[@]}" -f "${ROOT}/sql/stage4_ml_dashboard.hql" 2>&1 | tee "${ROOT}/output/stage4_hive.txt"
echo "stage4: Hive external tables refreshed (see output/stage4_hive.txt)."
