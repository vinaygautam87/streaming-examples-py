from pyspark.sql import SparkSession
import os.path


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

    # This is the schema of incoming data from Kafka:

    # StructType(
    # StructField(key,BinaryType,true),
    # StructField(value,BinaryType,true),
    # StructField(topic,StringType,true),
    # StructField(partition,IntegerType,true),
    # StructField(offset,LongType,true),
    # StructField(timestamp,TimestampType,true),
    # StructField(timestampType,IntegerType,true)
    # )
    #.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    consoleOutput = inputDf\
        .selectExpr("CAST(value AS STRING)")\
        .writeStream\
        .outputMode("append")\
        .format("console")\
        .start()

    consoleOutput.awaitTermination()