from pyspark.sql import SparkSession
import os.path
import yaml


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

    # Setup spark to use s3 : OPTION 1
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    inputDf = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "ec2-3-249-27-66.eu-west-1.compute.amazonaws.com:9092")\
        .option("subscribe", "test")\
        .load()
        # .option("kafka.bootstrap.servers", app_secret["kafka"]["server"])\
        # .option("subscribe", app_conf["kafka"]["topic"])\
        # .option("startingOffsets", "earliest") \

    consoleOutput = inputDf\
        .selectExpr("CAST(value AS STRING)")\
        .writeStream\
        .outputMode("append")\
        .format("console") \
        .start()\
        .awaitTermination()

# .option("checkpointLocation", "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["kafka"]["checkpoint_dir"]) \

    # spark-submit --packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0" com/dsm/kafka/append_mode_demo.py
