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

# Carregue os dados JSON com o schema acima
dataframe = spark.read.schema(schema).option("multiline", "true").json("s3://natalias-s3-bucket/Raw/Local/JSON/Movies/2023/05/09/*")

# Escreva os dados como parquet
dataframe.write.parquet("s3://natalias-s3-bucket/Trusted/Parquet/Movies/JSON/")




############ SCHEMA AUTOMATICO


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType, ArrayType

# Crie a sessão Spark
spark = SparkSession.builder.getOrCreate()

# Nome do bucket de saída
output_bucket = "natalias-s3-bucket"
output_folder = "Trusted/Parquet/Movies/JSON/"

# Carregue os dados JSON
dataframe = spark.read.option("multiline", "true").json("s3://natalias-s3-bucket/Raw/Local/JSON/Movies/2023/05/09/*")

# Escreva os dados como parquet
dataframe.write.mode("overwrite").parquet(f"s3://{output_bucket}/{output_folder}")


##### SCHEMA AUTOMÁTICO COM TRATAMENTO, MAS ESTÁ VINDO COMO ARRAY

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, translate, to_date, to_timestamp

# Crie a sessão Spark
spark = SparkSession.builder.getOrCreate()

# Carregue os dados JSON
dataframe = spark.read.option("multiline", "true").json("s3://natalias-s3-bucket/Raw/Local/JSON/Movies/2023/05/09/*")

# Infira o esquema automaticamente
dataframe = dataframe.selectExpr("*").limit(1)  # Leitura de uma linha para inferir o esquema
infer_schema = dataframe.schema

# Carregue novamente os dados JSON com o esquema inferido
dataframe = spark.read.option("multiline", "true").schema(infer_schema).json("s3://natalias-s3-bucket/Raw/Local/JSON/Movies/2023/05/09/*")
dataframe = dataframe.withColumn("movie", explode("movies"))#tentativa de nao virar array

# Trate valores nulos
dataframe = dataframe.na.fill(0)  # Preencha valores nulos com zero em todas as colunas

# Remova dados irrelevantes
dataframe = dataframe.drop("adult", "backdrop_path", "poster_path", "video", "vote_count")  # Descarte as colunas irrelevantes

# Remova registros duplicados
dataframe = dataframe.dropDuplicates()

# Nome do bucket de saída
output_bucket = "natalias-s3-bucket"
output_folder = "Trusted/Parquet/Movies/JSON2/"

# Salve o DataFrame processado em formato Parquet
dataframe.write.parquet(f"s3://{output_bucket}/{output_folder}")


#JSON 4 - TENTATIVA DE NAO VIRAR ARRAY
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

# Crie a sessão Spark
spark = SparkSession.builder.getOrCreate()

# Nome do bucket de saída
output_bucket = "natalias-s3-bucket"
output_folder = "Trusted/Parquet/Movies/JSON4/"

# Carregue os dados JSON
dataframe = spark.read.option("multiline", "true").option("mode", "PERMISSIVE").json("s3://natalias-s3-bucket/Raw/Local/JSON/Movies/2023/05/09/*.json")

# Exploda o array de registros em linhas individuais
dataframe = dataframe.select(explode("movies").alias("movie"))

# Escreva os dados como parquet
dataframe.write.mode("overwrite").parquet(f"s3://{output_bucket}/{output_folder}")
