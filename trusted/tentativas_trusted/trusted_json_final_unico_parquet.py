# Leia os arquivos JSON da pasta e inferir o esquema automaticamente
json_files = spark.read.option("inferSchema", "true").json(json_folder)

# Aplicar limpeza de valores nulos
json_files_cleaned = json_files.na.drop()

# Reparticionar os dados em uma única partição
json_files_repartitioned = json_files_cleaned.repartition(1)

# Escreva os dados como Parquet no bucket de saída dentro da pasta "trusted"
json_files_repartitioned.write.mode("overwrite").parquet(f"s3://{output_bucket}/{output_folder}/")
