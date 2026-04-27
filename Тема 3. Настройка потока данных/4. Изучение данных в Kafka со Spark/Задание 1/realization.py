from pyspark.sql import SparkSession

# ОБЯЗАТЕЛЬНО: переменная должна называться spark_jars_packages
spark_jars_packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"

# Настройки безопасности
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";',
}

# Создаём SparkSession
spark = SparkSession.builder \
    .master("local") \
    .appName("kafka_batch_read") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

# ГЛОБАЛЬНАЯ переменная df (пакетное чтение: read, НЕ readStream!)
df = (spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091")
      .options(**kafka_security_options)
      .option("subscribe", "persist_topic")
      .option("startingOffsets", "earliest")
      .load())