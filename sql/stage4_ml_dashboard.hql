USE team14_projectdb;

DROP TABLE IF EXISTS ml_feature_catalog;
CREATE EXTERNAL TABLE ml_feature_catalog (
    feature_name  STRING,
    feature_type  STRING,
    processing    STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar'     = '\"',
    'escapeChar'    = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/team14/project/stage4_catalog/ml_features'
TBLPROPERTIES ('skip.header.line.count' = '1');

DROP TABLE IF EXISTS ml_hyperparam_grid;
CREATE EXTERNAL TABLE ml_hyperparam_grid (
    model                STRING,
    parameter            STRING,
    search_space         STRING,
    best_selection_note  STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar'     = '\"',
    'escapeChar'    = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/team14/project/stage4_catalog/ml_hyper'
TBLPROPERTIES ('skip.header.line.count' = '1');

DROP TABLE IF EXISTS ml_eval_metrics;
CREATE EXTERNAL TABLE ml_eval_metrics (
    model      STRING,
    auc_roc    DOUBLE,
    auc_pr     DOUBLE,
    accuracy   DOUBLE,
    f1         DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar'     = '\"',
    'escapeChar'    = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/team14/project/output/evaluation.csv'
TBLPROPERTIES ('skip.header.line.count' = '1');

DROP TABLE IF EXISTS ml_pred_random_forest;
CREATE EXTERNAL TABLE ml_pred_random_forest (
    label       DOUBLE,
    prediction  DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar'     = '\"',
    'escapeChar'    = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/team14/project/output/model1_predictions'
TBLPROPERTIES ('skip.header.line.count' = '1');

DROP TABLE IF EXISTS ml_pred_linear_svc;
CREATE EXTERNAL TABLE ml_pred_linear_svc (
    label       DOUBLE,
    prediction  DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar'     = '\"',
    'escapeChar'    = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/team14/project/output/model2_predictions'
TBLPROPERTIES ('skip.header.line.count' = '1');

DROP TABLE IF EXISTS ml_pred_naive_bayes;
CREATE EXTERNAL TABLE ml_pred_naive_bayes (
    label       DOUBLE,
    prediction  DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar'     = '\"',
    'escapeChar'    = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/team14/project/output/model3_predictions'
TBLPROPERTIES ('skip.header.line.count' = '1');

SHOW TABLES;
