import boto3
import json
from datetime import datetime

# Nome do bucket S3
bucket_name = 'meu-ecommerce-dados'  # Substitua pelo nome do seu bucket
pasta_destino = 'pedidos/'           # Pasta dentro do bucket

# Inicializa clientes AWS
sqs = boto3.client('sqs', region_name='sa-east-1')  # ajuste a região
s3 = boto3.client('s3', region_name='sa-east-1')    # ajuste a região

# Nome da fila
queue_name = 'fila-pedidos-ecommerce'
queue_url = sqs.get_queue_url(QueueName=queue_name)['QueueUrl']

print(f"Escutando fila: {queue_url}")

while True:
    # Recebe até 1 mensagem com long polling
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=5
    )

    mensagens = response.get('Messages', [])

    if not mensagens:
        print("Nenhuma mensagem no momento...")
        continue

    for msg in mensagens:
        corpo = json.loads(msg['Body'])
        print(f"Pedido recebido: {corpo}")

        # Gera nome do arquivo no S3
        nome_arquivo = f"{pasta_destino}pedido_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"

        # Envia para o S3
        s3.put_object(
            Bucket=bucket_name,
            Key=nome_arquivo,
            Body=json.dumps(corpo),
            ContentType='application/json'
        )

        print(f"Pedido salvo no S3: s3://{bucket_name}/{nome_arquivo}")

        # Remove a mensagem da fila
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=msg['ReceiptHandle']
        )

        print("Mensagem removida da fila.\n")
