from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Inicialize a sessão do Spark
spark = SparkSession.builder.getOrCreate()

# Leia os arquivos Parquet diretamente do S3 e crie os dataframes
df_imdb = spark.read.parquet("s3://natalias-s3-bucket/Trusted/Parquet/Movies/CSV/")
df_tmdb = spark.read.parquet("s3://natalias-s3-bucket/Trusted/Parquet/Movies/JSON/")

# Restante do seu código...

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

# Salvar o DataFrame resultante no S3 em formato Parquet
result.write.parquet("s3://natalias-s3-bucket/Processed-Trusted/Parquet/Movies/resultedparquet")
