# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "08bf1610-8c6e-407c-be15-44a3d663ca8d",
# META       "default_lakehouse_name": "airlinesLH",
# META       "default_lakehouse_workspace_id": "12204d44-688c-40db-9593-4d23c9ad3e51",
# META       "known_lakehouses": [
# META         {
# META           "id": "08bf1610-8c6e-407c-be15-44a3d663ca8d"
# META         },
# META         {
# META           "id": "c50ef779-8988-48df-a781-5d972b1bc915"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import Window, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, OneHotEncoder, StringIndexer
from pyspark.ml.regression import GBTRegressionModel, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

table_name = "lhfinance.dbo.fact_finance_actuals_sample"  # adjust to your Lakehouse table name

raw_df = (
    spark.read.table(table_name)
    .withColumn("Month", F.to_date("Month"))
    .withColumn("ActualRevenueUSD", F.col("ActualRevenueUSD").cast("double"))
    .withColumn("BudgetRevenueUSD", F.col("BudgetRevenueUSD").cast("double"))
    .withColumn("BaselineForecastUSD", F.col("BaselineForecastUSD").cast("double"))
    .withColumn("EconomicIndex", F.col("EconomicIndex").cast("double"))
    .withColumn("MarketingSpendUSD", F.col("MarketingSpendUSD").cast("double"))
    .withColumn("Seasonality", F.col("Seasonality").cast("double"))
    .withColumn("EventFlag", F.col("EventFlag").cast("double"))
    .withColumn("MLTrainFlag", F.col("MLTrainFlag").cast("int"))
)

window_spec = Window.partitionBy("ProductLine").orderBy("Month")

feature_df = (
    raw_df
    .withColumn("RevenueLag1", F.lag("ActualRevenueUSD", 1).over(window_spec))
    .withColumn("RevenueLag3", F.lag("ActualRevenueUSD", 3).over(window_spec))
    .withColumn("RollingMean3", F.avg("ActualRevenueUSD").over(window_spec.rowsBetween(-2, 0)))
    .withColumn("BudgetVariance", F.col("ActualRevenueUSD") - F.col("BudgetRevenueUSD"))
    .withColumn("ForecastBias", F.col("ActualRevenueUSD") - F.col("BaselineForecastUSD"))
    .withColumn("MonthIndex", F.year("Month") * F.lit(12) + F.month("Month"))
    .na.drop()
)

train_df = feature_df.filter("MLTrainFlag = 1").cache()
holdout_df = feature_df.filter("MLTrainFlag = 0").cache()

categorical_cols = ["CompanyCode", "CostCenter", "ProductLine", "AccountCategory"]
numeric_cols = [
    "BudgetRevenueUSD", "BaselineForecastUSD", "EconomicIndex", "MarketingSpendUSD", "Seasonality",
    "EventFlag", "RevenueLag1", "RevenueLag3", "RollingMean3", "BudgetVariance", "ForecastBias", "MonthIndex"
]

indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="skip") for c in categorical_cols]
encoder = OneHotEncoder(
    inputCols=[f"{c}_idx" for c in categorical_cols],
    outputCols=[f"{c}_enc" for c in categorical_cols],
    handleInvalid="keep"
)
assembler = VectorAssembler(
    inputCols=numeric_cols + [f"{c}_enc" for c in categorical_cols],
    outputCol="features"
)

gbt = GBTRegressor(labelCol="ActualRevenueUSD", featuresCol="features", predictionCol="PredictedRevenueUSD")

ml_pipeline = Pipeline(stages=indexers + [encoder, assembler, gbt])
model = ml_pipeline.fit(train_df)

predictions = model.transform(holdout_df)

evaluator_rmse = RegressionEvaluator(
    labelCol="ActualRevenueUSD", predictionCol="PredictedRevenueUSD", metricName="rmse"
)
evaluator_mape = RegressionEvaluator(
    labelCol="ActualRevenueUSD", predictionCol="PredictedRevenueUSD", metricName="mae"
)

rmse = evaluator_rmse.evaluate(predictions)
mape = evaluator_mape.evaluate(predictions) / predictions.select("ActualRevenueUSD").agg(F.avg(F.col("ActualRevenueUSD"))).first()[0]

print(f"Holdout RMSE: {rmse:,.0f}")
print(f"Holdout MAPE (approx): {mape:.3%}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

predictions.write.mode("overwrite").format("delta").save("Tables/Gold/FactForecastPredictions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Window, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, OneHotEncoder, StringIndexer
from pyspark.ml.regression import GBTRegressionModel, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

table_name = "Gold.fact_finance_actuals"  # adjust to your Lakehouse table name

raw_df = (
    spark.read.table(table_name)
    .withColumn("Month", F.to_date("Month"))
    .withColumn("ActualRevenueUSD", F.col("ActualRevenueUSD").cast("double"))
    .withColumn("BudgetRevenueUSD", F.col("BudgetRevenueUSD").cast("double"))
    .withColumn("BaselineForecastUSD", F.col("BaselineForecastUSD").cast("double"))
    .withColumn("EconomicIndex", F.col("EconomicIndex").cast("double"))
    .withColumn("MarketingSpendUSD", F.col("MarketingSpendUSD").cast("double"))
    .withColumn("Seasonality", F.col("Seasonality").cast("double"))
    .withColumn("EventFlag", F.col("EventFlag").cast("double"))
    .withColumn("MLTrainFlag", F.col("MLTrainFlag").cast("int"))
)

window_spec = Window.partitionBy("ProductLine").orderBy("Month")

feature_df = (
    raw_df
    .withColumn("RevenueLag1", F.lag("ActualRevenueUSD", 1).over(window_spec))
    .withColumn("RevenueLag3", F.lag("ActualRevenueUSD", 3).over(window_spec))
    .withColumn("RollingMean3", F.avg("ActualRevenueUSD").over(window_spec.rowsBetween(-2, 0)))
    .withColumn("BudgetVariance", F.col("ActualRevenueUSD") - F.col("BudgetRevenueUSD"))
    .withColumn("ForecastBias", F.col("ActualRevenueUSD") - F.col("BaselineForecastUSD"))
    .withColumn("MonthIndex", F.year("Month") * F.lit(12) + F.month("Month"))
    .na.drop()
)

train_df = feature_df.filter("MLTrainFlag = 1").cache()
holdout_df = feature_df.filter("MLTrainFlag = 0").cache()

categorical_cols = ["CompanyCode", "CostCenter", "ProductLine", "AccountCategory"]
numeric_cols = [
    "BudgetRevenueUSD", "BaselineForecastUSD", "EconomicIndex", "MarketingSpendUSD", "Seasonality",
    "EventFlag", "RevenueLag1", "RevenueLag3", "RollingMean3", "BudgetVariance", "ForecastBias", "MonthIndex"
]

indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="skip") for c in categorical_cols]
encoder = OneHotEncoder(
    inputCols=[f"{c}_idx" for c in categorical_cols],
    outputCols=[f"{c}_enc" for c in categorical_cols],
    handleInvalid="keep"
)
assembler = VectorAssembler(
    inputCols=numeric_cols + [f"{c}_enc" for c in categorical_cols],
    outputCol="features"
)

gbt = GBTRegressor(labelCol="ActualRevenueUSD", featuresCol="features", predictionCol="PredictedRevenueUSD")

ml_pipeline = Pipeline(stages=indexers + [encoder, assembler, gbt])
model = ml_pipeline.fit(train_df)

predictions = model.transform(holdout_df)

evaluator_rmse = RegressionEvaluator(
    labelCol="ActualRevenueUSD", predictionCol="PredictedRevenueUSD", metricName="rmse"
)
evaluator_mape = RegressionEvaluator(
    labelCol="ActualRevenueUSD", predictionCol="PredictedRevenueUSD", metricName="mae"
)

rmse = evaluator_rmse.evaluate(predictions)
mape = evaluator_mape.evaluate(predictions) / predictions.select("ActualRevenueUSD").agg(F.avg(F.col("ActualRevenueUSD"))).first()[0]

print(f"Holdout RMSE: {rmse:,.0f}")
print(f"Holdout MAPE (approx): {mape:.3%}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

predictions.write.mode("overwrite").format("delta").save("Tables/dbo/FactForecastPredictions123")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
