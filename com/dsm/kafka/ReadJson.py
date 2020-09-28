from pyspark.sql import SparkSession
import os.path
from pyspark.sql.types import StructType,StructField, IntegerType, LongType,DoubleType,StringType,TimestampType
from pyspark.sql.functions import from_json,col,to_timestamp

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

    data_stream = sparkSession\
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

    # Watermark in order to handle late arriving data. Spark’s engine automatically tracks the current event time
    # and can filter out incoming messages if they are older than time T

    data_stream_transformed = data_stream\
        .withWatermark("timestamp","1 day")

    # Input stream json
    # {"firstName": "Quentin", "lastName": "Corkery", "birthDate": "1984-10-26T03:52:14.449+0000"}
    # {"firstName": "Neil", "lastName": "Macejkovic", "birthDate": "1971-08-06T18:03:11.533+0000"}

    person_schema = StructType([
        StructField("firstName", StringType(), True) ,
        StructField("lastName", StringType(), True) ,
        StructField("birthDate", StringType(), True)  ])

    data_stream_transformed = data_stream_transformed\
       .selectExpr("CAST(value AS STRING) as json")\
       .select(from_json(col("json"), person_schema).alias("person"))\
       .select("person.firstName", "person.lastName","person.birthDate")\
       .withColumn("birthDate", to_timestamp("birthDate", "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))

    data_stream_transformed.writeStream\
        .outputMode("append")\
        .format("console")\
        .option("truncate", "false")\
        .option("numRows", 30)\
        .start()\
        .awaitTermination()

