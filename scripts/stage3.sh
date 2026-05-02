#!/usr/bin/env bash
# Stage 3: Spark ML binary classification (YARN). Requires spark-submit on PATH (cluster edge node).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# spark-submit --master yarn needs Hadoop client XMLs. Non-login SSH shells often omit these.
if [ -z "${HADOOP_CONF_DIR:-}" ] && [ -z "${YARN_CONF_DIR:-}" ]; then
  for d in /etc/hadoop/conf /etc/hadoop /usr/lib/hadoop/etc/hadoop; do
    if [ -d "$d" ] && [ -f "$d/core-site.xml" ]; then
      export HADOOP_CONF_DIR="$d"
      export YARN_CONF_DIR="$d"
      break
    fi
  done
fi
if [ -z "${HADOOP_CONF_DIR:-}" ] && [ -n "${YARN_CONF_DIR:-}" ]; then
  export HADOOP_CONF_DIR="${YARN_CONF_DIR}"
fi
if [ -z "${YARN_CONF_DIR:-}" ] && [ -n "${HADOOP_CONF_DIR:-}" ]; then
  export YARN_CONF_DIR="${HADOOP_CONF_DIR}"
fi
if [ -z "${HADOOP_CONF_DIR:-}" ] && [ -z "${YARN_CONF_DIR:-}" ]; then
  echo "Set Hadoop config, e.g.:" >&2
  echo "  export HADOOP_CONF_DIR=/etc/hadoop/conf" >&2
  echo "  export YARN_CONF_DIR=/etc/hadoop/conf" >&2
  echo "Ask the course staff for the path on this host if unsure." >&2
  exit 1
fi

export TEAM="${TEAM:-team14}"
export SEED="${SEED:-42}"

exec spark-submit \
  --master yarn \
  --deploy-mode client \
  "$ROOT/scripts/stage3.py"
