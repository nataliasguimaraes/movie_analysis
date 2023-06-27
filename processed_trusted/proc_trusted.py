from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import boto3
session = boto3.Session(profile_name="***_AdministratorAccess",region_name="us-east-1")
s3 = boto3.resource('s3')

# Inicialize a sessão do Spark
spark = SparkSession.builder.getOrCreate()

# Leia os arquivos Parquet e crie os dataframes
df_imdb = spark.read.parquet("natalias-s3-bucket/Trusted/Parquet/Movies/CSV/")
df_tmdb = spark.read.parquet("natalias-s3-bucket/Trusted/Parquet/Movies/JSON/")

# Selecione as colunas necessárias do dataframe do IMDB
df_imdb = df_imdb.select(
    col("id").alias("idImdb"),
    col("anolancamento").alias("anoLancamento"),
    col("genero").alias("genero"),
    col("tituloprincipal").alias("tituloPrincipal"),
    col("notamedia").alias("notaMedia")
)

# Selecione as colunas necessárias do dataframe do TMDB
df_tmdb = df_tmdb.select(
    col("id").alias("idTmdb"),
    col("popularity").alias("popularity"),
    col("vote_average").alias("voteAverage"),
    col("vote_count").alias("voteCount"),
    col("release_date").alias("releaseDate")
)

# Crie a tabela FatoFilmes
df_fato_filmes = df_imdb.join(df_tmdb, "idImdb")

# Crie a tabela DimensaoTmdb
df_dimensao_tmdb = df_tmdb.select(
    col("idTmdb"),
    col("genre_ids").alias("generos"),
    col("original_language").alias("originalLanguage"),
    col("releaseDate")
)

# Crie a tabela DimensaoImdb
df_dimensao_imdb = df_imdb.select(
    col("idImdb"),
    col("anoLancamento"),
    col("genero"),
    col("tituloPrincipal")
)

# Salve os dataframes resultantes como tabelas temporárias
df_fato_filmes.createOrReplaceTempView("FatoFilmes")
df_dimensao_tmdb.createOrReplaceTempView("DimensaoTmdb")
df_dimensao_imdb.createOrReplaceTempView("DimensaoImdb")

# Execute uma consulta para visualizar os resultados
result = spark.sql("""
    SELECT
        FatoFilmes.idImdb,
        FatoFilmes.idTmdb,
        FatoFilmes.notaMedia,
        FatoFilmes.numeroVotos,
        FatoFilmes.popularity,
        FatoFilmes.voteAverage,
        FatoFilmes.voteCount,
        DimensaoTmdb.genres,
        DimensaoTmdb.originalLanguage,
        DimensaoTmdb.releaseDate,
        DimensaoImdb.anoLancamento,
        DimensaoImdb.genero,
        DimensaoImdb.tituloPrincipal
    FROM
        FatoFilmes
    JOIN
        DimensaoTmdb ON FatoFilmes.idTmdb = DimensaoTmdb.idTmdb
    JOIN
        DimensaoImdb ON FatoFilmes.idImdb = DimensaoImdb.idImdb
""")

# Salve o DataFrame resultante no S3 em formato Parquet
result.write.parquet("s3://natalias-s3-bucket/Processed-Trusted/Parquet/Movies/resultedparquet")