#!/usr/bin/env python3
"""
Stage III — Predictive analytics (binary classification: cancelled vs not).

Runs on YARN only (spark-submit --master yarn).
Reads Hive table team{N}_projectdb.flights_part, builds features, tunes three models
(RandomForestClassifier, LinearSVC, NaiveBayes) with 3x3x3 grids + 3-fold CV,
evaluates on held-out test with binary metrics (ROC + PR per course rules).

Writes artifacts to HDFS under hdfs:///user/{TEAM}/project/...
and mirrors CSV outputs into repo output/ via hdfs dfs -getmerge.
"""


import os
import subprocess
import sys
from pathlib import Path

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import (
    LinearSVC,
    NaiveBayes,
    RandomForestClassifier,
)
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _hdfs_base(team: str) -> str:
    return f"hdfs:///user/{team}/project"


def _run_hdfs_getmerge(hdfs_dir: str, local_file: Path) -> None:
    local_file.parent.mkdir(parents=True, exist_ok=True)
    cmd = f'hdfs dfs -getmerge "{hdfs_dir}" "{local_file}"'
    subprocess.run(cmd, shell=True, check=True)


def _string_indexer_label_count(si_model) -> int:
    """Return vocabulary size of a fitted StringIndexerModel (for RF maxBins)."""
    lbls = getattr(si_model, "labels", None)
    if lbls is not None:
        return int(len(lbls))
    lbls_arr = getattr(si_model, "labelsArray", None)
    if lbls_arr is not None:
        return int(len(lbls_arr))
    raise AttributeError("StringIndexerModel has neither .labels nor .labelsArray")


def build_spark(team: str) -> SparkSession:
    """Create a Hive-enabled SparkSession pinned to YARN with sensible memory defaults."""
    spark = (
        SparkSession.builder.appName(f"{team}-stage3-cancelled-classification")
        .master("yarn")
        .config(
            "spark.hadoop.hive.metastore.uris",
            "thrift://hadoop-02.uni.innopolis.ru:9883",
        )
        .config(
            "spark.sql.warehouse.dir",
            f"hdfs:///user/{team}/project/hive/warehouse",
        )
        .config("spark.executor.instances", os.environ.get("SPARK_EXECUTOR_INSTANCES", "4"))
        .config("spark.executor.cores", os.environ.get("SPARK_EXECUTOR_CORES", "3"))
        .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "4g"))
        .config(
            "spark.executor.memoryOverhead",
            os.environ.get("SPARK_EXECUTOR_MEMORY_OVERHEAD", "1536m"),
        )
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
        .config(
            "spark.driver.memoryOverhead",
            os.environ.get("SPARK_DRIVER_MEMORY_OVERHEAD", "1024m"),
        )
        .config("spark.dynamicAllocation.enabled", "false")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "512"))
        .config("spark.default.parallelism", os.environ.get("SPARK_DEFAULT_PARALLELISM", "128"))
        .enableHiveSupport()
        .getOrCreate()
    )
    if spark.sparkContext.master != "yarn":
        spark.stop()
        raise RuntimeError(
            "This job must run on YARN (spark-submit --master yarn). "
            f"Got master={spark.sparkContext.master!r}"
        )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_clean_frame(spark: SparkSession, hive_table: str):
    """Load Hive flights table and derive numeric label + calendar/time features."""
    df = spark.table(hive_table)
    base = (
        df.select(
            "airline_code",
            "origin",
            "dest",
            "fl_number",
            "distance",
            "fl_date",
            "crs_dep_time",
            "crs_arr_time",
            "cancelled",
        )
        .withColumn("label", F.col("cancelled").cast("int"))
        .drop("cancelled")
    )
    base = (
        base.withColumn(
            "flight_ts",
            F.from_unixtime((F.col("fl_date") / 1000).cast("long")).cast("timestamp"),
        )
        .withColumn("month", F.month("flight_ts"))
        .withColumn("day_of_week", F.dayofweek("flight_ts"))
        .drop("flight_ts")
    )
    base = base.withColumn("distance_km", F.col("distance").cast("double")).drop("distance")
    clean = base.filter(
        F.col("crs_dep_time").isNotNull()
        & F.col("crs_arr_time").isNotNull()
        & F.col("distance_km").isNotNull()
    )
    return clean


def make_ohe_indexers(cat_cols):
    """Build StringIndexer stages for high-cardinality categoricals before OHE."""
    return [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx_ohe", handleInvalid="keep")
        for c in cat_cols
    ]


def train_rf(clean, seed: int, hdfs_base: str):  # pylint: disable=too-many-locals
    """Tune RF on indexed features; write full pipeline and train/test splits to HDFS."""
    cat_cols = ["airline_code", "origin", "dest"]
    num_cols = [
        "fl_number",
        "distance_km",
        "month",
        "day_of_week",
        "crs_dep_time",
        "crs_arr_time",
    ]
    rf_indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx_rf", handleInvalid="keep")
        for c in cat_cols
    ]
    rf_assembler = VectorAssembler(
        inputCols=[f"{c}_idx_rf" for c in cat_cols] + num_cols,
        outputCol="features",
        handleInvalid="keep",
    )
    rf_prep_pipe = Pipeline(stages=rf_indexers + [rf_assembler])
    rf_prep_model = rf_prep_pipe.fit(clean)
    max_card = max(
        _string_indexer_label_count(m) + 1 for m in rf_prep_model.stages[:-1]
    )
    bins_lo = max_card
    bins_mid = max_card + 128
    bins_hi = max_card + 256

    rf_data = rf_prep_model.transform(clean).select("features", "label")
    rf_train, rf_test = rf_data.randomSplit([0.7, 0.3], seed=seed)
    rf_train = rf_train.cache()
    rf_test = rf_test.cache()

    e_auc_pr = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderPR",
    )
    rf_est = RandomForestClassifier(featuresCol="features", labelCol="label", seed=seed)
    rf_grid = (
        ParamGridBuilder()
        .addGrid(rf_est.maxDepth, [6, 10, 14])
        .addGrid(rf_est.maxBins, [bins_lo, bins_mid, bins_hi])
        .addGrid(rf_est.subsamplingRate, [0.5, 0.75, 1.0])
        .build()
    )
    rf_cv = CrossValidator(
        estimator=rf_est,
        estimatorParamMaps=rf_grid,
        evaluator=e_auc_pr,
        numFolds=3,
        parallelism=1,
    )
    rf_cv_model = rf_cv.fit(rf_train)
    rf_best = rf_cv_model.bestModel
    rf_full = PipelineModel(stages=[rf_prep_model, rf_best])

    out_dir = f"{hdfs_base}/models/model1"
    rf_full.write().overwrite().save(out_dir)

    pred = rf_best.transform(rf_test).select("label", "prediction")
    pred_path = f"{hdfs_base}/output/model1_predictions"
    pred.coalesce(1).write.mode("overwrite").format("csv").option("header", True).save(
        pred_path
    )

    rf_train.write.mode("overwrite").format("json").save(f"{hdfs_base}/data/train")
    rf_test.write.mode("overwrite").format("json").save(f"{hdfs_base}/data/test")

    return rf_full, rf_prep_model, rf_test, {
        "name": "RandomForest",
        "hdfs_model": out_dir,
        "hdfs_pred": pred_path,
    }


def train_svm(train_ohe, test_ohe, _seed: int, hdfs_base: str):
    """Tune LinearSVC on assembled OHE+numerics; write test predictions to HDFS."""
    e_auc_pr = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderPR",
    )
    svm = LinearSVC(
        featuresCol="features",
        labelCol="label",
        maxIter=100,
        standardization=True,
    )
    svm_grid = (
        ParamGridBuilder()
        .addGrid(svm.regParam, [0.01, 0.05, 0.2])
        .addGrid(svm.threshold, [0.35, 0.5, 0.65])
        .addGrid(svm.aggregationDepth, [2, 3, 4])
        .build()
    )
    svm_cv = CrossValidator(
        estimator=svm,
        estimatorParamMaps=svm_grid,
        evaluator=e_auc_pr,
        numFolds=3,
        parallelism=1,
    )
    svm_cv_model = svm_cv.fit(train_ohe)
    svm_best = svm_cv_model.bestModel

    pred = svm_best.transform(test_ohe).select("label", "prediction")
    pred_path = f"{hdfs_base}/output/model2_predictions"
    pred.coalesce(1).write.mode("overwrite").format("csv").option("header", True).save(
        pred_path
    )

    return svm_best, {
        "name": "LinearSVC",
        "hdfs_pred": pred_path,
    }


def train_nb(train_df, test_df, _seed: int, hdfs_base: str):
    """Tune Bernoulli/multinomial NB on OHE-only features; write test predictions to HDFS."""
    e_auc_pr = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderPR",
    )
    nb = NaiveBayes(featuresCol="features", labelCol="label", predictionCol="prediction")
    nb_grid = (
        ParamGridBuilder()
        .addGrid(nb.smoothing, [1e-4, 5e-4, 2e-3])
        .addGrid(
            nb.thresholds,
            [[0.30, 0.70], [0.40, 0.60], [0.45, 0.55]],
        )
        .addGrid(nb.modelType, ["bernoulli", "multinomial", "multinomial"])
        .build()
    )
    nb_cv = CrossValidator(
        estimator=nb,
        estimatorParamMaps=nb_grid,
        evaluator=e_auc_pr,
        numFolds=3,
        parallelism=1,
    )
    nb_cv_model = nb_cv.fit(train_df)
    nb_best = nb_cv_model.bestModel

    pred = nb_best.transform(test_df).select("label", "prediction")
    pred_path = f"{hdfs_base}/output/model3_predictions"
    pred.coalesce(1).write.mode("overwrite").format("csv").option("header", True).save(
        pred_path
    )

    return nb_best, {
        "name": "NaiveBayes",
        "hdfs_pred": pred_path,
    }


def evaluate_binary(_spark, model, test_df, model_label: str):
    """Return ROC, PR, accuracy, and F1 for a binary classifier on a feature DataFrame."""
    pred = model.transform(test_df).cache()
    roc = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    ).evaluate(pred)
    pr = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderPR",
    ).evaluate(pred)
    acc = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="accuracy",
    ).evaluate(pred)
    f1 = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="f1",
    ).evaluate(pred)
    pred.unpersist()
    return {
        "model": model_label,
        "auc_roc": float(roc),
        "auc_pr": float(pr),
        "accuracy": float(acc),
        "f1": float(f1),
    }


def sample_prediction_rf(rf_full: PipelineModel, clean):
    """Score one raw row with the saved RF pipeline (demonstrates end-to-end transform)."""
    one = clean.limit(1)
    out = rf_full.transform(one).select("label", "prediction")
    return out


def main() -> int:  # pylint: disable=too-many-locals,too-many-statements
    """Train three models, save to HDFS, copy CSV metrics into repo output/."""
    team = os.environ.get("TEAM", "team14").strip()
    seed = int(os.environ.get("SEED", "42"))
    hive_db = os.environ.get("HIVE_DB", f"{team}_projectdb")
    hive_table = os.environ.get("HIVE_TABLE", f"{hive_db}.flights_part")

    repo_root = _repo_root()
    out_dir = repo_root / "output"
    models_dir = repo_root / "models"
    out_dir.mkdir(parents=True, exist_ok=True)
    models_dir.mkdir(parents=True, exist_ok=True)

    hdfs_base = _hdfs_base(team)

    spark = build_spark(team)
    clean = load_clean_frame(spark, hive_table)

    cat_cols = ["airline_code", "origin", "dest"]
    num_cols = [
        "fl_number",
        "distance_km",
        "month",
        "day_of_week",
        "crs_dep_time",
        "crs_arr_time",
    ]

    # LinearSVC pipeline: separate indexer instances (do not reuse fitted stages).
    idx_ohe_a = make_ohe_indexers(cat_cols)
    enc_a = OneHotEncoder(
        inputCols=[f"{c}_idx_ohe" for c in cat_cols],
        outputCols=[f"{c}_ohe" for c in cat_cols],
    )
    asm_ohe = VectorAssembler(
        inputCols=[f"{c}_ohe" for c in cat_cols] + num_cols,
        outputCol="features",
        handleInvalid="keep",
    )
    prep_ohe = Pipeline(stages=idx_ohe_a + [enc_a, asm_ohe])
    prep_ohe_model = prep_ohe.fit(clean)
    data_ohe = prep_ohe_model.transform(clean).select("features", "label")
    train_ohe, test_ohe = data_ohe.randomSplit([0.7, 0.3], seed=seed)
    train_ohe = train_ohe.cache()
    test_ohe = test_ohe.cache()

    # NaiveBayes: OHE categoricals only -> features in {0,1}
    idx_ohe_b = make_ohe_indexers(cat_cols)
    enc_b = OneHotEncoder(
        inputCols=[f"{c}_idx_ohe" for c in cat_cols],
        outputCols=[f"{c}_ohe_nb" for c in cat_cols],
    )
    asm_nb = VectorAssembler(
        inputCols=[f"{c}_ohe_nb" for c in cat_cols],
        outputCol="features",
        handleInvalid="keep",
    )
    prep_nb = Pipeline(stages=idx_ohe_b + [enc_b, asm_nb])
    prep_nb_model = prep_nb.fit(clean)
    data_nb = prep_nb_model.transform(clean).select("features", "label")
    train_nb_df, test_nb_df = data_nb.randomSplit([0.7, 0.3], seed=seed)
    train_nb_df = train_nb_df.cache()
    test_nb_df = test_nb_df.cache()

    # --- Random Forest (full pipeline saved as model1) ---
    rf_full, _rf_prep, rf_test, rf_paths = train_rf(clean, seed, hdfs_base)
    rf_best = rf_full.stages[-1]
    # Metrics on the feature matrix: best classifier only (prep already applied in rf_test).
    metrics_rf = evaluate_binary(spark, rf_best, rf_test, "RandomForest")

    # --- LinearSVC: full PipelineModel on disk for scoring raw rows; metrics on test_ohe ---
    svm_best, svm_paths = train_svm(train_ohe, test_ohe, seed, hdfs_base)
    svm_full = PipelineModel(stages=[prep_ohe_model, svm_best])
    svm_model_hdfs = f"{hdfs_base}/models/model2"
    svm_full.write().overwrite().save(svm_model_hdfs)
    metrics_svm = evaluate_binary(spark, svm_best, test_ohe, "LinearSVC")

    # --- NaiveBayes ---
    nb_best, nb_paths = train_nb(train_nb_df, test_nb_df, seed, hdfs_base)
    nb_full = PipelineModel(stages=[prep_nb_model, nb_best])
    nb_model_hdfs = f"{hdfs_base}/models/model3"
    nb_full.write().overwrite().save(nb_model_hdfs)
    metrics_nb = evaluate_binary(spark, nb_best, test_nb_df, "NaiveBayes")

    rows = [metrics_rf, metrics_svm, metrics_nb]
    eval_df = spark.createDataFrame(rows)
    eval_hdfs = f"{hdfs_base}/output/evaluation.csv"
    eval_df.coalesce(1).write.mode("overwrite").format("csv").option("header", True).save(
        eval_hdfs
    )

    sample_df = sample_prediction_rf(rf_full, clean)
    sample_local = out_dir / "sample_prediction_rf.csv"
    sample_hdfs = f"{hdfs_base}/output/_tmp_sample_rf_pred"
    sample_df.coalesce(1).write.mode("overwrite").format("csv").option("header", True).save(
        sample_hdfs
    )
    try:
        _run_hdfs_getmerge(sample_hdfs, sample_local)
        subprocess.run(f'hdfs dfs -rm -r -f "{sample_hdfs}"', shell=True, check=False)
    except subprocess.CalledProcessError:
        rows = sample_df.collect()
        with open(sample_local, "w", encoding="utf-8") as fh:
            fh.write("label,prediction\n")
            for row in rows:
                fh.write(f"{row['label']},{row['prediction']}\n")

    # Pull deliverables to repo root output/ and models/ (optional copies for grading repo)
    try:
        _run_hdfs_getmerge(rf_paths["hdfs_pred"], out_dir / "model1_predictions.csv")
        _run_hdfs_getmerge(svm_paths["hdfs_pred"], out_dir / "model2_predictions.csv")
        _run_hdfs_getmerge(nb_paths["hdfs_pred"], out_dir / "model3_predictions.csv")
        _run_hdfs_getmerge(eval_hdfs, out_dir / "evaluation.csv")
        subprocess.run(
            f'hdfs dfs -get -f "{hdfs_base}/models/model1" "{models_dir / "model1"}"',
            shell=True,
            check=True,
        )
        subprocess.run(
            f'hdfs dfs -get -f "{svm_model_hdfs}" "{models_dir / "model2"}"',
            shell=True,
            check=True,
        )
        subprocess.run(
            f'hdfs dfs -get -f "{nb_model_hdfs}" "{models_dir / "model3"}"',
            shell=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        print(
            "Warning: could not hdfs-getmerge or hdfs-get some artifacts "
            f"(run on cluster with hdfs CLI): {exc}",
            file=sys.stderr,
        )

    print("Stage 3 metrics (test):")
    eval_df.show(truncate=False)
    print(
        "HDFS model paths:",
        rf_paths["hdfs_model"],
        svm_model_hdfs,
        nb_model_hdfs,
    )
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
