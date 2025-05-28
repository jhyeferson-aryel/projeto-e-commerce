# Para envio direto de pedidos para o S3
# Este script envia pedidos de um e-commerce para o Amazon S3 em formato JSON.
import boto3
import json
from datetime import datetime

# Inicializa o cliente S3
s3 = boto3.client('s3')

# Simula um pedido
pedido = {
    'usuario': 'Bruno',
    'produto': 'Notebook',
    'quantidade': 2,
    'valor_unitario': 2599.90,
    'data_pedido': datetime.now().isoformat()
}

# Nome do arquivo e do bucket
nome_arquivo = f"pedidos/pedido_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
bucket_name = "meu-ecommerce-dados"  # Substitua pelo seu nome real

# Envia para o S3
s3.put_object(
    Bucket=bucket_name,
    Key=nome_arquivo,
    Body=json.dumps(pedido),
    ContentType='application/json'
)

print(f"Pedido enviado com sucesso para s3://{bucket_name}/{nome_arquivo}")
