import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import boto3

# --- Configurações Iniciais ---
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --- 1. Leitura dos Dados (Extração) ---
# Lê a tabela criada pelo Crawler no seu banco de dados do Glue
# Certifique-se de que o nome do database e da tabela estão corretos
source_data = glueContext.create_dynamic_frame.from_catalog(
    database="ecommerce-db",
    table_name="pedidos", # O nome da tabela geralmente é o nome da pasta em minúsculas
    transformation_ctx="source_data",
)
print(f"Número de registros lidos da fonte: {source_data.count()}")

# --- 2. Transformação ---
# Converte o DynamicFrame do Glue para um DataFrame do Spark para facilitar a manipulação
dataframe = source_data.toDF()

# Exemplo de transformação: Cria uma nova coluna 'valor_total'
# Multiplicando a quantidade pelo valor_unitario
# A função 'withColumn' cria ou substitui uma coluna.
# 'col()' é usada para referenciar uma coluna existente e '.cast("decimal")' para garantir que são números
from pyspark.sql.functions import col

transformed_dataframe = dataframe.withColumn(
    "valor_total",
    col("quantidade") * col("valor_unitario")
)

# Reconverte o DataFrame do Spark para um DynamicFrame do Glue
transformed_dynamic_frame = DynamicFrame.fromDF(
    transformed_dataframe, glueContext, "transformed_dynamic_frame"
)

print("Transformação concluída. Schema final:")
transformed_dynamic_frame.printSchema()

# --- 3. Carregamento (Load) ---
# Define o caminho de destino no S3
output_path = "s3://meu-ecommerce-dados/processed/pedidos/"

# Salva os dados transformados no formato Parquet
glueContext.write_dynamic_frame.from_options(
    frame=transformed_dynamic_frame,
    connection_type="s3",
    format="parquet",
    connection_options={"path": output_path},
    transformation_ctx="sink_data",
)

print(f"Dados salvos com sucesso em: {output_path}")

job.commit()