from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType

# необходимая библиотека с идентификатором в maven
kafka_lib_id = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"

# настройки security для кафки
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
}

def spark_init() -> SparkSession:
    return SparkSession.builder \
        .master("local") \
        .appName("kafka_deduplication") \
        .config("spark.jars.packages", kafka_lib_id) \
        .getOrCreate()

def load_df(spark: SparkSession) -> DataFrame:
    # Безопасное получение TOPIC_NAME
    TOPIC_NAME = globals().get('TOPIC_NAME')
    if not TOPIC_NAME:
        try:
            from settings import TOPIC_NAME
        except ImportError:
            TOPIC_NAME = 'student.topic.cohort45.s24270523' # ваш топик по умолчанию
            
    return (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091")
            .options(**kafka_security_options)
            .option("subscribe", TOPIC_NAME)
            .option("startingOffsets", "latest")
            .load())

def transform(df: DataFrame) -> DataFrame:
    # 1. Схема для парсинга JSON из колонки value
    json_schema = StructType([
        StructField("client_id", StringType(), True),
        StructField("timestamp", StringType(), True), # Парсим как строку, потом конвертируем
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True)
    ])
    
    # 2. Извлечение данных из JSON
    parsed_df = df.select(
        f.from_json(f.col("value").cast("string"), json_schema).alias("data")
    ).select(
        f.col("data.client_id").alias("client_id"),
        f.to_timestamp(f.col("data.timestamp")).alias("timestamp"), # Конвертация в тип Timestamp
        f.col("data.lat").alias("lat"),
        f.col("data.lon").alias("lon")
    )
    
    # 3. Дедупликация с временным окном (Watermark)
    # Устанавливаем задержку 10 минут для удаления старых состояний
    deduped_df = (parsed_df
                  .withWatermark("timestamp", "10 minutes")
                  .dropDuplicates(["client_id", "timestamp"]))
                  
    return deduped_df

# === Основной блок (не меняйте) ===
spark = spark_init()

source_df = load_df(spark)
output_df = transform(source_df)
output_df.printSchema()

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