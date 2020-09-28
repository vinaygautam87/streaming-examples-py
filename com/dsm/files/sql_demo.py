from pyspark.sql import SparkSession
import yaml
import os.path
from pyspark.sql.functions import desc,sum as _sum
from pyspark.sql.types import *


if __name__ == '__main__':

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

    # Setup spark to use s3 : OPTION 1
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    data_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["files"]["directory"]

    schema = StructType([
        StructField("city_code", StringType(), True),
        StructField("city", StringType(), True),
        StructField("major_category", StringType(), True),
        StructField("minor_category", StringType(), True),
        StructField("value", IntegerType(), True),
        StructField("year", StringType(), True),
        StructField("month", StringType(), True)])

    raw_crime_df = spark.readStream \
        .option("header", "false") \
        .option("maxFilesPerTrigger", 2) \
        .schema(schema) \
        .csv(data_path)

    raw_crime_df.createOrReplaceTempView("CrimeData")

    print("Is the stream ready?", raw_crime_df.isStreaming)

    category_df = spark.sql("SELECT major_category, value FROM CrimeData WHERE year = '2016'")

    crime_per_cat_df = category_df.groupBy("major_category")\
        .agg(_sum("value").alias("convictions"))\
        .orderBy(desc("convictions"))

    query = crime_per_cat_df.writeStream\
        .outputMode("complete")\
        .format("console")\
        .option("truncate", "false")\
        .option("numRows", 30)\
        .start()\
        .awaitTermination()

