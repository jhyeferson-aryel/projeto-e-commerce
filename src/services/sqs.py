import boto3
import json

def enviar_pedido(pedido, nome_fila='fila-pedidos-ecommerce'):
    sqs = boto3.client('sqs', region_name='sa-east-1')
    queue_url = sqs.get_queue_url(QueueName=nome_fila)['QueueUrl']
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(pedido)
    )
