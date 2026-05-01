from datetime import datetime
from time import sleep

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType

# 🔥 Замените на ваши значения!
TOPIC_NAME_IN = 'student.topic.cohort45.s24270523'
TOPIC_NAME_91 = 'student.topic.cohort45.s24270523.out'

def spark_init(test_name) -> SparkSession:
    spark_jars_packages = ",".join([
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        "org.postgresql:postgresql:42.4.0",
    ])
    return (SparkSession.builder.appName(test_name)
        .config("spark.sql.session.timeZone", "UTC")  # 🔥 Добавлено
        .config("spark.jars.packages", spark_jars_packages)
        .getOrCreate())

postgresql_settings = {
    'user': 'student',
    'password': 'de-student'
}

def read_marketing(spark: SparkSession) -> DataFrame:
    return (spark.read
            .format("jdbc")
            .option("url", "jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de")
            .option("dbtable", "marketing_companies")  # 🔥 Без схемы
            .option("driver", "org.postgresql.Driver")
            .options(**postgresql_settings)
            .load())

kafka_security_options = {
    'kafka.bootstrap.servers':'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091',
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";',
}

def read_client_stream(spark: SparkSession) -> DataFrame:
    # 🔥 timestamp как DoubleType (epoch seconds)
    schema = StructType([
        StructField("client_id", StringType(), True),
        StructField("timestamp", DoubleType(), True),  # 🔥 Исправлено
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
    ])
    
    return (spark.readStream.format('kafka')
          .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
          .option("subscribe", TOPIC_NAME_IN)
          .options(**kafka_security_options)
          .option("maxOffsetsPerTrigger", 1000)  # 🔥 Добавлено
          .option("startingOffsets", "earliest")  # 🔥 Чтобы прочитать тестовые данные
          .load()
          .withColumn('value', f.col('value').cast(StringType()))
          .withColumn('event', f.from_json(f.col('value'), schema))
          .selectExpr('event.*')
          # 🔥 Конвертация времени из epoch
          .withColumn('timestamp',
                      f.from_unixtime(f.col('timestamp'), "yyyy-MM-dd' 'HH:mm:ss.SSS").cast(TimestampType()))
          # 🔥 Сначала дедуп, потом watermark
          .dropDuplicates(['client_id', 'timestamp'])
          .withWatermark('timestamp', '10 minutes')
          )

def join(user_df, marketing_df) -> DataFrame:
    return (user_df
            .crossJoin(marketing_df)
            .withColumn("adv_campaign_id", marketing_df.id)
            .withColumn("adv_campaign_description", marketing_df.description)
            .withColumn("adv_campaign_start_time", marketing_df.start_time)
            .withColumn("adv_campaign_end_time", marketing_df.end_time)
            .withColumn("client_id", f.substring('client_id', 0, 6))
            .withColumn("created_at", f.lit(datetime.now()))
            # 🔥 Формула Хаверсина
            .withColumn("a", (
                f.pow(f.sin(f.radians(marketing_df.point_lat - user_df.lat) / 2), 2) +
                f.cos(f.radians(user_df.lat)) * f.cos(f.radians(marketing_df.point_lat)) *
                f.pow(f.sin(f.radians(marketing_df.point_lon - user_df.lon) / 2), 2)))
            .withColumn("distance", (f.atan2(f.sqrt(f.col("a")), f.sqrt(-f.col("a") + 1)) * 12742000))
            .withColumn("distance", f.col('distance').cast(IntegerType()))
            .where(f.col("distance") <= 1000)
            # 🔥 Вторая дедупликация
            .dropDuplicates(['client_id', 'adv_campaign_id'])
            # 🔥 Watermark после join
            .withWatermark('timestamp', '1 minutes')
            # 🔥 Только 7 обязательных полей в JSON
            .select(f.to_json(f.struct(
                'client_id',
                'distance',
                'adv_campaign_id',
                'adv_campaign_description',
                'adv_campaign_start_time',
                'adv_campaign_end_time',
                'created_at'
            )).alias('value'))
    )

if __name__ == "__main__":
    spark = spark_init('join stream')
    client_stream = read_client_stream(spark)
    marketing_df = read_marketing(spark)
    output = join(client_stream, marketing_df)
    
    query = (output
             .writeStream
             .outputMode("append")
             .format("kafka")
             .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
             .options(**kafka_security_options)
             .option("topic", TOPIC_NAME_91)
             .option("checkpointLocation", "test_query")  # 🔥 Добавлено
             .trigger(processingTime="15 seconds")  # 🔥 Непрерывный режим
             .start())

    while query.isActive:
        print(f"query information: runId={query.runId}, status is {query.status}")
        sleep(30)
    query.awaitTermination()