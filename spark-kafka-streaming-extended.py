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
    .option("subscribe", "tweetTestIncreased") \
    .load() \
    .select(from_json(col("value").cast("string"), jsonSchema).alias("message")) \
    .select(["message.created_at","message.text"])

cnt = 30

DF = []
for i in range(0,10*cnt,10):
    partition = '{"tweetTestIncreased":['
    for j in range(10) :
        partition += str(i+j)
        if(j!=9):
            partition += ','
        else:
            partition += ']}'

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("assign", partition) \
        .load() \
        .select(from_json(col("value").cast("string"), jsonSchema).alias("message")) \
        .select(["message.created_at","message.text"])

    DF.append(df)

consoleSinks = []
for i in range(cnt):
    name = "kafka_spark_console" + str(i+1)
    consoleSink = DF[i] \
        .writeStream \
        .queryName(name)\
        .format("console") \
        .option("truncate", "false") \
        .start()
    consoleSinks.append(consoleSink)

memorySink = df \
    .writeStream \
    .queryName("kafka_spark_memory")\
    .format("memory") \
    .start()

# spark.streams.active   # 실행 중인 스트림 목록

# spark.sql("select * from kafka_spark_memory").show(cnt,False)

for i in range(cnt):
    consoleSinks[i].stop()

memorySink.stop()
