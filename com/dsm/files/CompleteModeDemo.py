from pyspark.sql import SparkSession
import yaml
import os.path
from pyspark.sql.functions import desc
from pyspark.sql.types import StructType,StructField, StringType
from com.dsm.streaming.utils.constants import ACCESS_KEY,SECRET_ACCESS_KEY,S3_BUCKET


if __name__ == '__main__':

    # Create the SparkSession
    sparkSession = SparkSession \
        .builder \
        .appName("Streaming Example") \
        .master('local[*]') \
        .getOrCreate()


    current_dir = os.path.abspath(os.path.dirname(__file__))
    appConfigFilePath = os.path.abspath(current_dir + "/../"+"application.yml")

    with open(appConfigFilePath) as conf:
        doc = yaml.load(conf,Loader=yaml.FullLoader)

    # Setup spark to use s3 : OPTION 1
    hadoop_conf = sparkSession.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", doc["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", doc["s3_conf"]["secret_access_key"])
    hadoop_conf.set("fs.s3a.endpoint", "s3-eu-west-1.amazonaws.com")

    # Setup spark to use s3 :Using Constants : OPTION 2
    hadoop_conf2 = sparkSession.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf2.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf2.set("fs.s3a.access.key", ACCESS_KEY)
    hadoop_conf2.set("fs.s3a.secret.key", SECRET_ACCESS_KEY)
    hadoop_conf2.set("fs.s3a.endpoint", "s3-eu-west-1.amazonaws.com")
    
    dataPath = "/00_MyDrive/ApacheSpark/InteljiWorkspace/streaming-examples/src/main/resources/datasets/droplocation"
    dataPathS3 ="s3a://"+S3_BUCKET+"/datasets/droplocation"

    schema = StructType([
        StructField("city_code", StringType(), True) ,
        StructField("city", StringType(), True) ,
        StructField("major_category", StringType(), True) ,
        StructField("minor_category", StringType(), True) ,
        StructField("value", StringType(), True) ,
        StructField("year", StringType(), True) ,
        StructField("month", StringType(), True) ])

    fileStreamDF = sparkSession.readStream \
      .option("header", "false") \
      .option("maxFilesPerTrigger", 2) \
      .schema(schema) \
      .csv(dataPath)

    print("Is the stream ready?" , fileStreamDF.isStreaming)

    fileStreamDF.printSchema()

    recordsPerCity = fileStreamDF\
        .groupBy("city")\
        .count()\
        .orderBy(desc("count"))

    #OutputMode in which all the rows in the streaming DataFrame/Dataset will be written to the sink every time there are some updates.
    #Complete mode does not drop old aggregation state and preserves all data in the Result Table.
    query = recordsPerCity.writeStream\
      .outputMode("complete")\
      .format("console")\
      .option("truncate", "false")\
      .option("numRows", 30)\
      .start()\
      .awaitTermination()