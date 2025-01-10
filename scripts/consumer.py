import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType


ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")
RESULTS_FOLDER  = os.getenv("RESULTS_FOLDER")
CHECKPOINTS_FOLDER  = os.getenv("CHECKPOINTS_FOLDER")

def create_spark_session():
    return SparkSession \
        .builder \
        .appName("TicketAnalysis") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINTS_FOLDER) \
        .config("spark.sql.shuffle.partitions", "12") \
        .config("spark.default.parallelism", "12") \
        .config("spark.streaming.backpressure.enabled", "true") \
        .config("spark.streaming.kafka.maxRatePerPartition", "100") \
        .getOrCreate()

def process_stream(spark):
    # Définition du schéma
    ticket_schema = StructType([
        StructField("id", StringType(), True),
        StructField("client_id", StringType(), True),
        StructField("demande", StringType(), True),
        StructField("demande_type", StringType(), True),
        StructField("prority", StringType(), True),
        StructField("create_time", StringType(), True)
    ])

    # Lecture du stream
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "redpanda-0:9092,redpanda-1:9092,redpanda-2:9092") \
        .option("subscribe", "client_tickets") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", "10000") \
        .load()

    # Parsing et conversion du timestamp
    parsed_df = df.select(
        from_json(col("value").cast("string"), ticket_schema).alias("data")
    ).select("data.*")

    # Convertir create_time en timestamp AVANT d'appliquer le watermark
    parsed_df = parsed_df.withColumn(
        "create_time",
        to_timestamp(col("create_time"))
    )

    # Fonction d'assignation
    def assign_team(demande_type):
        if demande_type == "Demande type 1":
            return "Équipe A"
        elif demande_type == "Demande type 2":
            return "Équipe B"
        return "Équipe C"

    # Enregistrer la fonction UDF
    from pyspark.sql.functions import udf
    assign_team_udf = udf(assign_team, StringType())

    # Enrichissement et watermark
    enriched_df = parsed_df \
        .withColumn("support_team", assign_team_udf(col("demande_type"))) \
        .withWatermark("create_time", "1 minute")

    # Agrégation par fenêtre d'une minute
    result = enriched_df \
        .groupBy("demande_type", "support_team", window(col("create_time"), "1 minute")).count()

    # Sauvegarde de données dans AWS S3
    return result \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", RESULTS_FOLDER) \
        .option("checkpointLocation", CHECKPOINTS_FOLDER) \
        .option("compression", "snappy") \
        .partitionBy("support_team") \
        .trigger(processingTime='1 minute') \
        .start()

def main():
    spark = None
    query = None
    
    try:
        spark = create_spark_session()
        query = process_stream(spark)
        
        if query:
            query.awaitTermination()
    except Exception as e:
        print(f"Erreur critique: {e}")
    finally:
        if query:
            query.stop()
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()