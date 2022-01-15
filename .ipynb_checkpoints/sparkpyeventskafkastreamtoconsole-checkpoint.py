from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

customerStediSchema = StructType([
    StructField("customer", StringType()),
    StructField("score", StringType()),
    StructField("riskDate", StringType())
])


spark = SparkSession.builder \
    .appName("stedi-stream-app") \
    .getOrCreate()

spark.sparkContext \
    .setLogLevel("WARN")


stediEventsRawDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stedi-events") \
    .option("startingOffets", "earliest") \
    .load()


stediEventsDF = stediEventsRawDF.selectExpr("CAST(key as string) key", "CAST(value as string) value")


stediEventsDF.withColumn("value", from_json("value", customerStediSchema)) \
    .select(col("value.*")) \
    .createOrReplaceTempView("CustomerRisk")


customerRiskStreamingDF = spark.sql("""
    SELECT
        customer,
        score
    FROM CustomerRisk
""")


customerRiskStreamingDF.writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination()