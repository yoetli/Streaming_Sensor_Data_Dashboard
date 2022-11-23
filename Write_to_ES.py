import findspark
import warnings
warnings.filterwarnings('ignore')
from elasticsearch import Elasticsearch, helpers
import time

findspark.init("/opt/manual/spark")

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

spark = (SparkSession.builder
         .appName("Kafka to Elasticsearch")
         .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:7.12.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1")
         .getOrCreate())

spark.sparkContext.setLogLevel('ERROR')

officeinput_map =  {
  "settings": {
    "index": {
      "analysis": {
        "analyzer": {
          "custom_analyzer":
          {
            "type":"custom",
            "tokenizer":"standard",
            "filter":[
              "lowercase", "custom_edge_ngram","asciifolding"
            ]
          }
        },
        "filter": {
          "custom_edge_ngram": {
            "type": "edge_ngram",
            "min_gram":2,
            "max_gram": 10
            }
          }
        }
      }
    },
    "mappings": {
    "properties": {
      "event_ts_min":  { "type": "date" },
      "ts_min_bignt":  { "type": "integer" },
      "room":   { "type": "keyword"  },
      "co2": {"type": "float"},
      "light": { "type":   "float"  },
      "temperature": {"type": "float"},
      "humidity": {"type": "float"},
      "pir": {"type": "float"},
    }
  }
  }

es = Elasticsearch("http://localhost:9200")

# try:
#     es.indices.delete("officeinput3")
#     print("officeinput3  index deleted.")
# except:
#     print("No index")

# es.indices.create(index="officeinput3", body=officeinput_map)

df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "office-input2")#kafka topic'i
      .option("failOnDataLoss",False)
      .load())

df2 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic","partition","offset", "timestamp")

df3 = df2.withColumn("event_ts_min", F.split(F.col("value"), ",")[0].cast(TimestampType())) \
.withColumn("ts_min_bignt", F.split(F.col("value"), ",")[1].cast(IntegerType())) \
.withColumn("room", F.split(F.col("value"), ",")[2].cast(StringType())) \
.withColumn("co2", F.split(F.col("value"), ",")[3].cast(IntegerType())) \
.withColumn("light", F.split(F.col("value"), ",")[4].cast(FloatType())) \
.withColumn("temperature", F.split(F.col("value"), ",")[5].cast(FloatType())) \
.withColumn("humidity", F.split(F.col("value"), ",")[6].cast(FloatType())) \
.withColumn("pir", F.split(F.col("value"), ",")[7].cast(FloatType())) \
.drop("value")

checkpoint_dir = "file:///tmp/streaming/read_from_kafka_write_to_es_e"

start_time = time.time()

streamingQuery = (df3.writeStream
                  .format("org.elasticsearch.spark.sql")
                  .outputMode("append") # stream'de overwrite yapilmaz.
                  .trigger(processingTime="3 second")
                  .option("numRows", 4)
                  .option("truncate", False)
                  .option("checkpointLocation", checkpoint_dir )
                  .option("es.resource","officeinput3") #Elestik search index'i
                  .option("es.nodes", "localhost:9200")
                  .start())

print("----- %s secs -----" %(time.time() - start_time))

streamingQuery.awaitTermination()