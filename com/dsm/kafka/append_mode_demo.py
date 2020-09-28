from pyspark.sql import SparkSession
import os.path


if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0" pyspark-shell'
    )

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read from enterprise applications") \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    inputDf = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("subscribe", "items-topic")\
        .option("startingOffsets", "earliest")\
        .load()

    consoleOutput = inputDf\
        .selectExpr("CAST(value AS STRING)")\
        .writeStream\
        .outputMode("append")\
        .format("console")\
        .start()\
        .awaitTermination()

# spark-submit --packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0" com/dsm/kafka/append_mode_demo.py
