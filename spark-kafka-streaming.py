from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession


from pyspark.sql.functions import *

spark = SparkSession.builder \
    .master("local") \
    .appName("Structured Streaming") \
    .getOrCreate()

# sc = SparkContext('local', 'PySparkShell')

jsonSchema = spark\
  .read \
  .option("multiLine", True) \
  .json("./tw1.json") \
  .schema

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "tweetTest") \
    .load() \
    .select(from_json(col("value").cast("string"), jsonSchema).alias("message")) \
    .select(["message.created_at","message.text"])

consoleSink = df \
    .writeStream \
    .queryName("kafka_spark_console")\
    .format("console") \
    .option("truncate", "false") \
    .start()

memorySink = df \
  .writeStream \
  .queryName("kafka_spark_memory")\
  .format("memory") \
  .start()

# spark.streams.active   # 실행 중인 스트림 목록

consoleSink.stop()
memorySink.stop()
