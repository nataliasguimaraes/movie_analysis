import boto3

s3 = boto3.resource('s3')

bucket_name = 'natalias-s3-bucket'
file_path = 'C:\\Users\\natal\\python.zip'
object_key = 'python.zip'

s3.meta.client.upload_file(file_path, bucket_name, object_key)