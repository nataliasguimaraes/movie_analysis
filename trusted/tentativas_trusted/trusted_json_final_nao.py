import boto3
from botocore.exceptions import ClientError
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession

# Crie o contexto Spark e Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Caminho da pasta contendo os arquivos JSON de entrada
json_folder = "s3://natalias-s3-bucket/Raw/Local/JSON/Movies/2023/05/09/"

# Nome do bucket de saída
output_bucket = "natalias-s3-bucket"

# Caminho de saída para os arquivos Parquet dentro do bucket
output_folder = "Trusted/Parquet/Movies/JSON/"

# Caminho completo para a pasta "trusted"
trusted_folder = f"{output_folder}/trusted/"

# Leia os arquivos JSON da pasta e inferir o esquema automaticamente
json_files = spark.read.option("inferSchema", "true").json(json_folder)

# Escreva os dados como Parquet no bucket de saída dentro da pasta "trusted"
json_files.coalesce(1).write.mode("overwrite").parquet(f"s3://{output_bucket}/{output_folder}/")
