from pyspark.sql import SparkSession

# Библиотека для подключения к PostgreSQL через JDBC
postgres_lib = "org.postgresql:postgresql:42.4.2"

# Создаём SparkSession с подключением библиотеки
spark = (
    SparkSession.builder
    .appName("postgres_jdbc_task")
    .config("spark.jars.packages", postgres_lib)
    .getOrCreate()
)

# Чтение данных из PostgreSQL
df = (spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de")
      .option("driver", "org.postgresql.Driver")
      .option("user", "student")
      .option("password", "de-student")
      .option("dbtable", "public.marketing_companies")  # схема.таблица
      .load())

# Считаем количество строк
print(f"Количество строк: {df.count()}")