import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession

# Crie a sessão Spark
spark = SparkSession.builder.getOrCreate()

# Caminho do arquivo CSV de entrada
movies_csv_file = "s3://natalias-s3-bucket/Raw/Local/CSV/Movies/2023/04/26/movies.csv"

# Nome e caminho do bucket de saída
output_bucket = "natalias-s3-bucket"
output_folder = "Trusted/Parquet/Movies/"

# Caminho completo para a pasta "trusted"
trusted_folder = f"{output_folder}/trusted/"

# Verifique se a pasta "trusted" já existe no bucket de saída
s3_client = boto3.client("s3")
try:
    s3_client.head_object(Bucket=output_bucket, Key=f"{trusted_folder}dummy-file")
except ClientError:
    # A pasta "trusted" não existe, então crie-a
    s3_client.put_object(Bucket=output_bucket, Key=f"{trusted_folder}dummy-file")

# Leia os dados CSV inferindo o esquema automaticamente
movies_read = spark.read.option("header", True).option("sep", "|").option("inferSchema", True).csv(movies_csv_file)

# Escreva os dados como Parquet no bucket de saída
movies_read.coalesce(1).write.mode("overwrite").parquet(f"s3://{output_bucket}/{movies_parquet_path}")
