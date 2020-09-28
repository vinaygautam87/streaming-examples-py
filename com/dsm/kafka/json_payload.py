from pyspark.sql import SparkSession
import os.path
import yaml
from pyspark.sql.types import *
from pyspark.sql.functions import *

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

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    data_stream = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("subscribe", "items-topic")\
        .load()

    # Watermark in order to handle late arriving data. Spark’s engine automatically tracks the current event time
    # and can filter out incoming messages if they are older than time T

    data_stream_transformed = data_stream\
        .withWatermark("timestamp", "1 day")

    # Input stream json
    # {"firstName": "Quentin", "lastName": "Corkery", "birthDate": "1984-10-26T03:52:14.449+0000"}
    # {"firstName": "Neil", "lastName": "Macejkovic", "birthDate": "1971-08-06T18:03:11.533+0000"}

    person_schema = StructType([
        StructField("firstName", StringType(), True),
        StructField("lastName", StringType(), True),
        StructField("birthDate", StringType(), True)])

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

# spark-submit --packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0" com/dsm/kafka/json_payload.py
