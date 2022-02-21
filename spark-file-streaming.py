from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession


from pyspark.sql.functions import *

spark = SparkSession.builder \
    .master("local") \
    .appName("Structured Streaming") \
    .getOrCreate()

# sc = SparkContext('local', 'PySparkShell')

static = spark.read.json("hdfs://localhost:9000/spark-streaming-data/data/activity-data")
dataSchema = static.schema

streaming = spark.readStream.schema(dataSchema)\
    .option("maxFilesPerTrigger", 1)\
    .json("hdfs://localhost:9000/spark-streaming-data/data/activity-data")  # "maxFilesPerTrigger"로 트리거가 실행되기까지 읽는 파일 개수 설정

# streaming.isStreaming    
activityCounts = streaming.groupBy("gt").count()  # transformation
spark.conf.set("spark.sql.shuffle.partitions", 5)  # shuffle partition 조정

activityQuery = activityCounts.writeStream\
    .queryName("activity_counts")\
    .format("memory")\
    .outputMode("complete")\
    .start()

# activityQuery.awaitTermination()

from time import sleep

for _ in range(30):
    spark.sql("SELECT * FROM activity_counts").show()
    sleep(1)

activityQuery.stop()
