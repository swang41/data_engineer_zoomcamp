-- CREATE A ML TABLE WITH APPROPRIATE TYPE
CREATE OR REPLACE TABLE `ny_taxi_data.yellow_tripdata_ml` (
  `passenger_count` INTEGER,
  `trip_distince` FLOAT64,
  `PULocationID` STRING,
  `DOLocationID` STRING,
  `payment_type` STRING,
  `fare_amount` FLOAT64,
  `tolls_amount` FLOAT64,
  `tip_amount` FLOAT64
) AS (
  SELECT passenger_count, trip_distance, CAST(PULocationID AS STRING), CAST(DOLocationID AS STRING), CAST(payment_type AS STRING), fare_amount, tolls_amount, tip_amount FROM `ny_taxi_data.yellow_tripdata_partitioned` WHERE fare_amount != 0
);

-- CREATE MODEL WITH DEFAULT SETTING
CREATE OR REPLACE MODEL `ny_taxi_data.tip_model`
OPTIONS
  (model_type='linear_reg',
  input_label_cols=['tip_amount'],
  DATA_SPLIT_METHOD='AUTO_SPLIT') AS 
SELECT
*
FROM `ny_taxi_data.yellow_tripdata_ml`
WHERE tip_amount IS NOT NULL;

-- CHECK FEATURES
SELECT * FROM ML.FEATURE_INFO(MODEL `ny_taxi_data.tip_model`);

-- EVALUATE THE MODEL
SELECT
*
FROM
  ML.EVALUATE(MODEL `ny_taxi_data.tip_model`,
  (
    SELECT
    *
    FROM `ny_taxi_data.yellow_tripdata_ml`
    WHERE tip_amount IS NOT NULL
  ));

-- PREDICT THE MODEL
SELECT
*
FROM
  ML.PREDICT(MODEL `ny_taxi_data.tip_model`,
  (
    SELECT
    *
    FROM `ny_taxi_data.yellow_tripdata_ml`
    WHERE tip_amount IS NOT NULL
  ));

-- PREDICT AND EXPLAIN
SELECT
*
FROM
  ML.EXPLAIN_PREDICT(MODEL `ny_taxi_data.tip_model`,
  (
    SELECT
    *
    FROM `ny_taxi_data.yellow_tripdata_ml`
    WHERE tip_amount IS NOT NULL
  ), STRUCT(4 as top_k_features));

-- CREATE MODEL WITH HYPERTUNNING
CREATE OR REPLACE MODEL `ny_taxi_data.tip_hyperparam_model`
OPTIONS
  (model_type='linear_reg',
  input_label_cols=['tip_amount'],
  DATA_SPLIT_METHOD='AUTO_SPLIT',
  num_trials=5,
  max_parallel_trials=2,
  l1_reg=hparam_range(0, 20),
  l2_reg=hparam_candidates([0, 0.1, 1, 10])) AS 
SELECT
*
FROM `ny_taxi_data.yellow_tripdata_ml`
WHERE tip_amount IS NOT NULL;
