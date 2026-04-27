from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType

# необходимая библиотека с идентификатором в maven
kafka_lib_id = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"

# настройки security для кафки
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";',
}

def spark_init() -> SparkSession:
    return SparkSession.builder \
        .master("local") \
        .appName("streaming_transform") \
        .config("spark.jars.packages", kafka_lib_id) \
        .getOrCreate()


def load_df(spark: SparkSession) -> DataFrame:
    TOPIC_NAME = globals().get('TOPIC_NAME')
    if not TOPIC_NAME:
        try:
            from settings import TOPIC_NAME
        except ImportError:
            TOPIC_NAME = 'student.topic.cohort45.s24270523'
            
    return (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091")
            .options(**kafka_security_options)
            .option("subscribe", TOPIC_NAME)
            .option("startingOffsets", "latest")  # ← Поменяйте на latest
            .load())


def transform(df: DataFrame) -> DataFrame:
    # 1. Схема с явным указанием nullable=True (третий аргумент True)
    json_schema = StructType([
        StructField("client_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True)
    ])

    # 2. Парсим JSON в новую колонку 'parsed'
    parsed_df = df.withColumn("parsed", f.from_json(f.col("value").cast("string"), json_schema))

    # 3. Достаем поля. 
    # ВАЖНО: Убрали .cast() для lat/lon/client_id, чтобы сохранить nullable=True из схемы.
    # Для timestamp оставляем to_timestamp, так как он нужен для конвертации типа и он возвращает nullable=True.
    return parsed_df.select(
        f.col("parsed.client_id"),
        f.to_timestamp(f.col("parsed.timestamp")).alias("timestamp"),
        f.col("parsed.lat"),
        f.col("parsed.lon")
    )

# ==========================================
# ГЛОБАЛЬНАЯ ОБЛАСТЬ (СТРОГО без отступов!)
# ==========================================
spark = spark_init()
source_df = load_df(spark)
output_df = transform(source_df)

query = (output_df
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