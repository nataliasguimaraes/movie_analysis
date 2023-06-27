from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType, ArrayType

# Crie a sessão Spark
spark = SparkSession.builder.getOrCreate()

# Defina o esquema manualmente
schema = StructType([
    StructField("adult", BooleanType(), True),
    StructField("backdrop_path", StringType(), True),
    StructField("id", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("original_language", StringType(), True),
    StructField("original_title", StringType(), True),
    StructField("overview", StringType(), True),
    StructField("poster_path", StringType(), True),
    StructField("media_type", StringType(), True),
    StructField("genre_ids", ArrayType(IntegerType()), True),
    StructField("popularity", DoubleType(), True),
    StructField("release_date", StringType(), True),
    StructField("video", BooleanType(), True),
    StructField("vote_average", DoubleType(), True),
    StructField("vote_count", IntegerType(), True)
])

# Carregue os dados JSON com o esquema especificado
dataframe = spark.read.schema(schema).option("multiline", "true").json("s3://natalias-s3-bucket/Raw/Local/JSON/Movies/2023/05/09/*")

# Escreva os dados como parquet
dataframe.write.parquet("s3://natalias-s3-bucket/Trusted/Parquet/Movies/JSON/")