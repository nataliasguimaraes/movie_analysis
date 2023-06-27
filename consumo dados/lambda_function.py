import json
import requests
from datetime import datetime

import pandas as pd
import boto3

#FILTER CSV INTO AWS BUCKET

def lambda_handler(event, context):
    bucket_name = 'natalias-s3-bucket'
    s3_file_name = 'Raw/Local/CSV/Movies/2023/04/26/movies.csv'
    s3_client = boto3.client('s3')
    s3 = boto3.resource('s3')
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_file_name)
    df_movies = pd.read_csv(response['Body'], sep='|', na_values=['\\N', 'NA'])

    df_filtered = df_movies[df_movies['genero'].isin(['Romance'])]

    filtered_ids = df_filtered['id'].tolist()

#ITERATE FILTERED IDS WITH TMDB API

    api_key = '***'
    url_base = 'https://api.themoviedb.org/3/movie/{movie_id}?api_key={api_key}&language=pt-BR"'


    batch_size = 1000
    num_batches = (len(filtered_ids) + batch_size - 1) // batch_size

    for i in range(num_batches):
        batch_ids = filtered_ids[i*batch_size : (i+1)*batch_size]
        movies = []
        for movie_id in batch_ids:
            url = url_base.format(movie_id=movie_id, api_key=api_key)
            response = requests.get(url)
            if response.status_code == 200:
                movie_data = json.loads(response.content)
                movies.append(movie_data)

        data = {"movies": movies}

        json_data = json.dumps(data, ensure_ascii=False, indent=4)

        file_name = f"tmdb_fmovies_{i+1}.json"

# WRITE JSON DATA INTO AWS BUCKET

        bucket_movies_directory = f'Raw/Local/JSON/Movies/{datetime.now().strftime("%Y/%m/%d")}/{file_name}'
        s3.Object(bucket_name, bucket_movies_directory).put(Body=json_data)
