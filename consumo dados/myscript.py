import boto3
session = boto3.Session(profile_name="***_AdministratorAccess",region_name="us-east-1")
s3 = boto3.resource('s3')
s3.create_bucket(Bucket='natalias-s3-bucket')

# 1. ler os 2 arquivos (filmes e series) no formato CSV inteiros, ou seja, sem filtrar os dados
import csv

with open("movies.csv", encoding='utf-8') as movies_file:
    print(movies_file.read())

with open("series.csv", encoding='utf-8') as series_file:
    print(series.csv.read())


# 2. utilizar a lib boto3 para carregar os dados para a AWS e acessar a AWS e grava no S3, no bucket definido com RAW Zone
from datetime import datetime

bucket_movies_directory = f'Raw/Local/CSV/Movies/{datetime.now().strftime("%Y/%m/%d")}/movies.csv'
s3.meta.client.upload_file("movies.csv", 'natalias-s3-bucket', bucket_movies_directory)

bucket_series_directory = f'Raw/Local/CSV/Series/{datetime.now().strftime("%Y/%m/%d")}/series.csv'
s3.meta.client.upload_file("series.csv", 'natalias-s3-bucket', bucket_series_directory)