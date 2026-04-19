from pyspark.sql import SparkSession
from pyspark.sql import functions as f, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

kafka_lib_id = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"

kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";',
}

def spark_init() -> SparkSession:
    return SparkSession.builder \
        .appName("kafka_task") \
        .config("spark.jars.packages", kafka_lib_id) \
        .getOrCreate()


def load_df(spark: SparkSession) -> DataFrame:
    kafka_bootstrap = "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091"
    topic_name = "student.topic.cohort41.mark9891"  # ← ЗАМЕНИТЕ НА СВОЙ!
    
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", topic_name) \
        .options(**kafka_security_options) \
        .option("ssl.ca.location", "/app/certs/CA.pem") \
        .option("startingOffsets", "earliest") \
        .load()


def transform(df: DataFrame) -> DataFrame:
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("album", StringType(), True),
        StructField("artist", StringType(), True),
        StructField("sold", StringType(), True)
    ])
    
    return df.selectExpr("CAST(value AS STRING) as json_value") \
        .select(f.from_json(f.col("json_value"), schema).alias("data")) \
        .select("data.*")


spark = spark_init()
source_df = load_df(spark)
df = transform(source_df)

df.printSchema()
df.show(truncate=False)