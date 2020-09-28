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

    inputDf = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", app_secret["kafka"]["server"])\
        .option("subscribe", app_conf["kafka"]["topic"])\
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
