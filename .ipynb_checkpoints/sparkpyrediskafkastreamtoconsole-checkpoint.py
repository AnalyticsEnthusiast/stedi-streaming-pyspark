from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

redisSchema = StructType([
    StructField("key", StringType()),
    StructField("existType", StringType()),
    StructField("ch", StringType()),
    StructField("incr", StringType()),
    StructField("zSetEntries", ArrayType(
        StructType([
            StructField("element", StringType()),
            StructField("score", StringType())
        ])
    ))
])

customerSchema = StructType([
    StructField("customerName", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("birthDay", StringType())
])


spark = SparkSession.builder \
    .appName("redis-stream-app") \
    .getOrCreate()

spark.sparkContext \
    .setLogLevel("WARN")

redisServerRawDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "redis-server") \
    .option("startingOffsets", "earliest") \
    .load()


redisServerDF = redisServerRawDF.selectExpr("CAST(key as string) key", "CAST(value as string) value")


redisServerDF.withColumn("value", from_json("value", redisSchema)) \
    .select(col("value.*")) \
    .createOrReplaceTempView("RedisSortedSet")


redisEncodedDF = spark.sql("""
    SELECT
        key,
        zSetEntries[0].element as encodedCustomer
    FROM RedisSortedSet
""")


redisDecodedDF = redisEncodedDF.withColumn("decodedCustomer", unbase64(redisEncodedDF.encodedCustomer).cast("string"))


redisDecodedDF.withColumn("decodedCustomer", from_json("decodedCustomer", customerSchema)) \
    .select(col("decodedCustomer.*")) \
    .createOrReplaceTempView("CustomerRecords")


emailAndBirthDayStreamingDF = spark.sql("""
    SELECT 
        email,
        birthDay
    FROM CustomerRecords
    WHERE email IS NOT NULL 
    AND birthDay IS NOT NULL
""")


emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.select("email", split(emailAndBirthDayStreamingDF.birthDay, "-").getItem(0).alias("birthYear"))


emailAndBirthYearStreamingDF.writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination()