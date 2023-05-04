from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import col
from cassandra.cluster import Cluster
import uuid

# define schema
schema = StructType([
    StructField("device", StringType(), True),
    StructField("bandi_market", DoubleType(), True),
    StructField("city_market", DoubleType(), True),
    StructField("grocery_shop", DoubleType(), True)])

# create spark session
spark = SparkSession \
    .builder \
    .appName("Stream Handler") \
    .config("spark.cassandra.connection.host", "localhost") \
    .getOrCreate()

# read from Kafka
inputDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crop") \
    .load()

# only select 'value' from the table,
# convert from bytes to string
rawDF = inputDF \
    .selectExpr("CAST(value AS STRING)") \
    .select(col("value").cast("string"))

# split each row on comma, load it to the dataframe
expandedDF = rawDF \
    .selectExpr("split(value, ',')[1] as device", "split(value, ',')[2] as bandi_market", \
                "split(value, ',')[3] as city_market", "split(value, ',')[4] as grocery_shop") \
    .selectExpr("device", "cast(bandi_market as double) as bandi_market", \
                "cast(city_market as double) as city_market", "cast(grocery_shop as double) as grocery_shop")

# groupby and aggregate
summaryDf = expandedDF \
    .groupBy("device") \
    .agg(avg("bandi_market"), avg("city_market"), avg("grocery_shop"))

# create a user-defined function that creates UUIDs
def makeUUID():
    return str(uuid.uuid1())

# register the UDF
make_uuid_udf = udf(lambda: makeUUID())

# add the UUIDs and renamed the columns
# this is necessary so that the dataframe matches the 
# table schema in cassandra
summaryWithIDs = summaryDf \
    .withColumn("uuid", make_uuid_udf()) \
    .withColumnRenamed("avg(bandi_market)", "bandi_market") \
    .withColumnRenamed("avg(city_market)", "city_market") \
    .withColumnRenamed("avg(grocery_shop)", "grocery_shop") 

# write dataframe to Cassandra
summaryWithIDs \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .start() \
    .awaitTermination()

