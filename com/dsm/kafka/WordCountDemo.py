from pyspark.sql import SparkSession
import yaml
import os.path
from pyspark.sql.functions import desc,explode, split,count
from pyspark.sql.types import StructType,StructField, IntegerType, LongType,DoubleType,StringType,TimestampType


if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0" pyspark-shell'
    )

    # Create the SparkSession
    sparkSession = SparkSession \
        .builder \
        .appName("Read from enterprise applications") \
        .master('local[*]') \
        .getOrCreate()

    inputDf = sparkSession\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("subscribe", "items-topic")\
        .option("startingOffsets", "earliest")\
        .load()

    consoleOutput = inputDf\
        .selectExpr("CAST(value AS STRING)")\
        .withColumn("value", split("value", " "))\
        .withColumn("value", explode("value"))\
        .groupBy("value")\
        .agg(count("value"))\
        .writeStream\
        .outputMode("complete")\
        .format("console")\
        .start()\

    consoleOutput.awaitTermination()