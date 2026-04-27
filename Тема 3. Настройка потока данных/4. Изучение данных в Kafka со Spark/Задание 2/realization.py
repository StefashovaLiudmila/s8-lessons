from pyspark.sql import SparkSession
from pyspark.sql import functions as f, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, LongType, TimestampType

# ОБЯЗАТЕЛЬНО: переменная должна называться spark_jars_packages
spark_jars_packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"

# настройки security для кафки
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
}

def spark_init() -> SparkSession:
    """Инициализация SparkSession с подключением библиотеки Kafka"""
    return SparkSession.builder \
        .master("local") \
        .appName("kafka_json_deserialize") \
        .config("spark.jars.packages", spark_jars_packages) \
        .getOrCreate()


def load_df(spark: SparkSession) -> DataFrame:
    """Чтение данных из топика persist_topic (пакетный режим)"""
    return (spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091")
            .options(**kafka_security_options)
            .option("subscribe", "persist_topic")
            .option("startingOffsets", "earliest")
            .load())


def transform(df: DataFrame) -> DataFrame:
    """Десериализация JSON из колонки value + сохранение метаданных Kafka"""
    
    # Схема для парсинга JSON внутри поля value
    json_schema = StructType([
        StructField("subscription_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("currency", StringType(), True)
    ])
    
    # Парсим JSON и объединяем с метаданными Kafka
    return (df.select(
                f.from_json(f.col("value").cast("string"), json_schema).alias("parsed"),
                f.col("key").cast("string").alias("key"),
                f.col("value").cast("string").alias("value"),
                f.col("topic"),
                f.col("partition"),
                f.col("offset"),
                f.col("timestamp"),
                f.col("timestampType")
            )
            .select(
                f.col("parsed.subscription_id"),
                f.col("parsed.name"),
                f.col("parsed.description"),
                f.col("parsed.price"),
                f.col("parsed.currency"),
                f.col("key"),
                f.col("value"),
                f.col("topic"),
                f.col("partition"),
                f.col("offset"),
                f.col("timestamp"),
                f.col("timestampType")
            ))


# === Основной блок ===
spark = spark_init()
source_df = load_df(spark)
df = transform(source_df)

df.printSchema()
df.show(truncate=False)