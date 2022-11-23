from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

spark = (SparkSession.builder
         .appName("Final_hw_read_example")
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1")
         .getOrCreate())

spark.sparkContext.setLogLevel('ERROR')

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "office-input2") \
  .option("failOnDataLoss",False)  \
  .load()

checkpointDir = "file:///tmp/streaming/kafka-office-input"

df2 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic","partition","offset", "timestamp")

df3 = df2.withColumn("event_ts_min", F.split(F.col("value"), ",")[0].cast(StringType())) \
.withColumn("ts_min_bignt", F.split(F.col("value"), ",")[1].cast(IntegerType())) \
.withColumn("room", F.split(F.col("value"), ",")[2].cast(StringType())) \
.withColumn("co2", F.split(F.col("value"), ",")[3].cast(IntegerType())) \
.withColumn("light", F.split(F.col("value"), ",")[4].cast(FloatType())) \
.withColumn("temperature", F.split(F.col("value"), ",")[5].cast(FloatType())) \
.withColumn("humidity", F.split(F.col("value"), ",")[6].cast(FloatType())) \
.withColumn("pir", F.split(F.col("value"), ",")[7].cast(FloatType())) \
.drop("value")

streamingQuery = (df3.writeStream
                  .format("console")
                  .outputMode("append")
                  .trigger(processingTime="5 second")
                  .option("numRows", 5)
                  .option("truncate", False)
                  .option("checkpointLocation", checkpointDir)
                  .start())

streamingQuery.awaitTermination()