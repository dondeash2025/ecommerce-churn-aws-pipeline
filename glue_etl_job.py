{\rtf1\ansi\ansicpg1252\cocoartf2868
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fnil\fcharset0 Menlo-Regular;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs24 \cf0 import sys\
from awsglue.transforms import *\
from awsglue.utils import getResolvedOptions\
from pyspark.context import SparkContext\
from awsglue.context import GlueContext\
from awsglue.job import Job\
from pyspark.sql import functions as F\
from pyspark.sql.types import IntegerType, DoubleType, StringType\
\
args = getResolvedOptions(sys.argv, ['JOB_NAME'])\
sc = SparkContext()\
glueContext = GlueContext(sc)\
spark = glueContext.spark_session\
job = Job(glueContext)\
job.init(args['JOB_NAME'], args)\
\
# \uc0\u9472 \u9472  1. READ \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \
df = spark.read.option("header", "true").option("inferSchema", "true").csv(\
    "s3://ecommerce-churn-pipeline-ad/raw/ECommerce_Dataset.csv"\
)\
\
print(f"Raw row count: \{df.count()\}")\
df.printSchema()\
\
# \uc0\u9472 \u9472  2. CAST TYPES \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \
df = df.withColumn("CustomerID", F.col("CustomerID").cast(IntegerType())) \\\
       .withColumn("Churn", F.col("Churn").cast(IntegerType())) \\\
       .withColumn("Tenure", F.col("Tenure").cast(DoubleType())) \\\
       .withColumn("CityTier", F.col("CityTier").cast(IntegerType())) \\\
       .withColumn("SatisfactionScore", F.col("SatisfactionScore").cast(IntegerType())) \\\
       .withColumn("NumberOfDeviceRegistered", F.col("NumberOfDeviceRegistered").cast(IntegerType())) \\\
       .withColumn("NumberOfAddress", F.col("NumberOfAddress").cast(IntegerType())) \\\
       .withColumn("Complain", F.col("Complain").cast(IntegerType()))\
\
# \uc0\u9472 \u9472  3. NULL IMPUTATION (median strategy) \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \
null_cols = [\
    "Tenure", "WarehouseToHome", "HourSpendOnApp",\
    "OrderAmountHikeFromlastYear", "CouponUsed",\
    "OrderCount", "DaySinceLastOrder"\
]\
\
for col in null_cols:\
    median_val = df.approxQuantile(col, [0.5], 0.001)[0]\
    df = df.fillna(\{col: median_val\})\
    print(f"Filled \{col\} nulls with median: \{median_val\}")\
\
# \uc0\u9472 \u9472  4. ADD DERIVED COLUMNS \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \
df = df.withColumn("HighValueCustomer",\
        F.when(F.col("CashbackAmount") > 200, 1).otherwise(0)) \\\
       .withColumn("ChurnRisk",\
        F.when(F.col("Complain") == 1, "High")\
         .when(F.col("SatisfactionScore") <= 2, "Medium")\
         .otherwise("Low"))\
\
# \uc0\u9472 \u9472  5. VERIFY \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \
print(f"Processed row count: \{df.count()\}")\
for col in null_cols:\
    null_count = df.filter(F.col(col).isNull()).count()\
    print(f"  \{col\}: \{null_count\} nulls remaining")\
\
# \uc0\u9472 \u9472  6. WRITE PARQUET \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \u9472 \
df.write.mode("overwrite").parquet(\
    "s3://ecommerce-churn-pipeline-ad/processed/"\
)\
\
print("Parquet write complete.")\
job.commit()}