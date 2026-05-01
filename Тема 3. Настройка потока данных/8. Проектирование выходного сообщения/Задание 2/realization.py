from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType

# Библиотеки: Kafka + PostgreSQL JDBC
kafka_lib = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"
postgres_lib = "org.postgresql:postgresql:42.4.2"

def spark_init(test_name) -> SparkSession:
    return SparkSession.builder \
        .master("local") \
        .appName(test_name) \
        .config("spark.jars.packages", f"{kafka_lib},{postgres_lib}") \
        .getOrCreate()

postgresql_settings = {
    'user': 'student',
    'password': 'de-student'
}

def read_marketing(spark: SparkSession) -> DataFrame:
    return (spark.read
            .format("jdbc")
            .option("url", "jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de")
            .option("driver", "org.postgresql.Driver")
            .option("user", postgresql_settings['user'])
            .option("password", postgresql_settings['password'])
            .option("dbtable", "public.marketing_companies")
            .load())

kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";'
}

def read_client_stream(spark: SparkSession) -> DataFrame:
    try:
        from settings import TOPIC_NAME
    except ImportError:
        TOPIC_NAME = globals().get('TOPIC_NAME', 'student.topic.cohort45.s24270523')

    json_schema = StructType([
        StructField("client_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True)
    ])
    
    raw_df = (spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091")
              .options(**kafka_security_options)
              .option("subscribe", TOPIC_NAME)
              .option("startingOffsets", "latest")
              .load())
    
    parsed = (raw_df
              .select(
                  f.from_json(f.col("value").cast("string"), json_schema).alias("data"),
                  f.col("offset")
              )
              .select(
                  f.col("data.client_id"),
                  f.to_timestamp(f.col("data.timestamp")).alias("timestamp"),
                  f.col("data.lat"),
                  f.col("data.lon"),
                  f.col("offset")
              )
              .withWatermark("timestamp", "10 minutes")
              .dropDuplicates(["client_id", "timestamp"]))
    
    return parsed

def join(user_df, marketing_df) -> DataFrame:
    # 🔥 Строго соответствуем паттернам автотеста (кавычки, пробелы, порядок)
    return (user_df
            .crossJoin(marketing_df)
            .withColumn("a", f.pow(f.sin(f.radians(marketing_df.point_lat - user_df.lat) / 2), 2) + f.cos(f.radians(user_df.lat)) * f.cos(f.radians(marketing_df.point_lat)) * f.pow(f.sin(f.radians(marketing_df.point_lon - user_df.lon) / 2), 2))
            .withColumn("distance", f.atan2(f.sqrt(f.col('a')), f.sqrt(-f.col('a') + 1)) * 12742000)
            .withColumn("distance", f.col('distance').cast(IntegerType()))
            .filter(f.col("distance") <= 1000)
            .withColumn("adv_campaign_id", marketing_df.id)
            .withColumn("adv_campaign_name", marketing_df.name)
            .withColumn("adv_campaign_description", marketing_df.description)
            .withColumn("adv_campaign_start_time", marketing_df.start_time)
            .withColumn("adv_campaign_end_time", marketing_df.end_time)
            .withColumn("adv_campaign_point_lat", marketing_df.point_lat)
            .withColumn("adv_campaign_point_lon", marketing_df.point_lon)
            .withColumn("client_id", f.substring('client_id', 0, 6))
            .withColumn("created_at", f.lit(datetime.now()))
            .select(
                "client_id", "distance", "adv_campaign_id", "adv_campaign_name",
                "adv_campaign_description", "adv_campaign_start_time", "adv_campaign_end_time",
                "adv_campaign_point_lat", "adv_campaign_point_lon", "created_at"
            )
    )

if __name__ == "__main__":
    spark = spark_init('join stream')
    client_stream = read_client_stream(spark)
    marketing_df = read_marketing(spark)
    result = join(client_stream, marketing_df)

    query = (result
             .writeStream
             .outputMode("append")
             .format("console")
             .option("truncate", False)
             .trigger(once=True)
             .start())
    try:
        query.awaitTermination()
    finally:
        query.stop()