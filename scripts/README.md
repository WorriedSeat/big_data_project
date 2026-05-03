## Contains `.sh` `.py` scripts of the pipeline

Stage 4 (`stage4.sh`): uploads `sql/data/stage4_ml_*.csv` to HDFS and runs `sql/stage4_ml_dashboard.hql` so Superset can read `ml_*` Hive tables (plus existing `q*_results` / `flights_part`). Local smoke: `python scripts/stage4_validate.py` (also covered by `pylint scripts` on the grader host).