from pyspark.sql import SparkSession
import yaml
import os.path
from pyspark.sql.types import *


if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Streaming Example") \
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

    # Setup spark to use s3 : OPTION 1
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    data_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/droplocation"

    schema = StructType([
        StructField("city_code", StringType(), True) ,
        StructField("city", StringType(), True) ,
        StructField("major_category", StringType(), True) ,
        StructField("minor_category", StringType(), True) ,
        StructField("value", StringType(), True) ,
        StructField("year", StringType(), True) ,
        StructField("month", StringType(), True) ])

    fileStreamDF = spark.readStream \
        .option("header", "false") \
        .option("maxFilesPerTrigger", 2) \
        .schema(schema) \
        .csv(data_path)

    print("Is the stream ready?" , fileStreamDF.isStreaming)

    fileStreamDF.printSchema()

    trimmedDF = fileStreamDF\
        .filter("city = 'Southwark' and year='2011' ")\
        .select("city", "year", "month", "value")\
        .withColumnRenamed("value", "convictions")

    # OutputMode in which only the new rows in the streaming DataFrame/Dataset will be written to the sink.
    query = trimmedDF.writeStream\
        .outputMode("append")\
        .format("console")\
        .option("truncate", "false")\
        .option("numRows", 30)\
        .start()\
        .awaitTermination()

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" com/dsm/files/append_mode_demo.py
