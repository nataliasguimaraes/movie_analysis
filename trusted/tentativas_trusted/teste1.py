from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

from pyspark.context import SparkContext
#from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col


# Crie a sessão Spark
spark = SparkSession.builder.getOrCreate()

# Defina o esquema manualmente
schema = StructType([
    StructField("id", StringType(), True),
    StructField("titulopincipal", StringType(), True),
    StructField("titulooriginal", StringType(), True),
    StructField("anolancamento", LongType(), True),
    StructField("tempominutos", StringType(), True),
    StructField("genero", StringType(), True),
    StructField("notamedia", DoubleType(), True),
    StructField("numerovotos", LongType(), True),
    StructField("generoartista", StringType(), True),
    StructField("personagem", StringType(), True),
    StructField("nomeartista", StringType(), True),
    StructField("anonascimento", StringType(), True),
    StructField("anofalecimento", StringType(), True),
    StructField("profissao", StringType(), True),
    StructField("titulosmaisconhecidos", StringType(), True),
    StructField("anotermino", StringType(), True)
])

# Carregue os dados CSV com o esquema especificado
dataframe = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load("/natalia-s3-bucket/Raw/Local/CSV/Movies/2023/04/26/movies.csv")

# Tratamento de valores nulos
numeric_columns = ["tituloPincipal", "tituloOriginal", "anoLancamento", "tempoMinutos", "genero", "notaMedia", "numeroVotos", "generoArtista"
                   "personagem", "nomeArtista", "anoNascimento", "anoFalecimento", "profissao", "titulosMaisConhecidos"]
for column in numeric_columns:
    dataframe = dataframe.withColumn(column, when(col(column).isNull(), 0).otherwise(col(column)))

# Tratamento de valores nulos em colunas de texto
string_columns = ["genero", "generoArtista", "personagem", "nomeArtista", "profissao", "titulosMaisConhecidos"]
for column in string_columns:
    dataframe = dataframe.withColumn(column, when(col(column).isNull(), "null").otherwise(col(column)))


# Escreva os dados como Parquet
dataframe.write.parquet("s3://natalia-s3-bucket/Trusted/Parquet/Movies/2023/05/29/")