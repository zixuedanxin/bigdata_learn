from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, from_json, window
from pyspark.sql.types import StructType, IntegerType, StringType, TimestampType


spark = SparkSession.builder.master(
    "local[2]"
).getOrCreate()

stream_data = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092") \
    .option("subscribe", "hydd_log") \
    .load()
stream_data.printSchema()

# kafka json数据解析
data_schema = StructType().add(
    "msg", StringType()
).add(
    "count", IntegerType()
).add(
    "create_time", TimestampType()
)
new_stream_data = stream_data.select(
    stream_data.key.cast("string"),
    from_json(stream_data.value.cast("string"), data_schema).alias('json_data')
)
new_stream_data.printSchema()

# msg按空格分隔
word_df = new_stream_data.select(
    explode(split(new_stream_data.json_data.msg, " ")).alias('word'),
    (new_stream_data.json_data.create_time).alias('create_time')
)
word_df.printSchema()

# 聚合
wordCounts = word_df.withWatermark(
    'create_time', '1 minutes'
).groupBy(
    window(word_df.create_time, '1 minutes', '1 minutes'),
    word_df.word
).count().orderBy('window')

# Start running the query that prints the running counts to the console
query = wordCounts \
    .writeStream \
    .trigger(processingTime='1 minutes') \
    .outputMode("complete") \
    .format("console") \
    .option('truncate', 'false') \
    .start() \
    .awaitTermination()


new_stream_data = stream_data.select(
    stream_data.key.cast("string"),
    from_json(stream_data.value.cast("string"), data_schema).alias('json_data')
)
new_stream_data.printSchema()

# msg按空格分隔
word_df = new_stream_data.select(
    explode(split(new_stream_data.json_data.msg, " ")).alias('word'),
    (new_stream_data.json_data.create_time).alias('create_time')
)
word_df.printSchema()

# 聚合
wordCounts = word_df.withWatermark(
    'create_time', '1 minutes'
).groupBy(
    window(word_df.create_time, '1 minutes', '1 minutes'),
    word_df.word
).count()

# Start running the query that prints the running counts to the console
query = wordCounts \
    .selectExpr("CAST(window AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .outputMode("update") \
    .trigger(processingTime='1 minutes') \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.33.50:9092") \
    .option("topic", "testres") \
    .option("checkpointLocation", "/tmp/testres") \
    .start() \
    .awaitTermination()

