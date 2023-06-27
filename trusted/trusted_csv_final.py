import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession

# Crie a sessão Spark
spark = SparkSession.builder.getOrCreate()

# Caminho do arquivo CSV de entrada
movies_csv_file = "s3://natalias-s3-bucket/Raw/Local/CSV/Movies/2023/04/26/movies.csv"

# Nome do bucket de saída
output_bucket = "natalias-s3-bucket"
output_folder = "Trusted/Parquet/Movies/CSV/"

# Caminho de saída para os arquivos Parquet dentro do bucket
movies_parquet_path = f"{output_folder}trusted-movies-csv-imdb.parquet"

# Leia os dados CSV com esquema automatico
movies_read = spark.read.option("header", True).option("sep", "|").option("inferSchema", True).csv(movies_csv_file)

movies_read_final = movies_read.withColumnRenamed("tituloPincipal", "tituloPrincipal").withColumnRenamed("id", "id_IMDB")

movies_cleaned = movies_read_final.na.drop()

# Escreva os dados como Parquet no bucket de saída
movies_cleaned.coalesce(1).write.mode("overwrite").parquet(f"s3://{output_bucket}/{movies_parquet_path}")
