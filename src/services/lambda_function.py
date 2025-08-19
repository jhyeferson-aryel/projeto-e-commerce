import boto3
import json
import os
from datetime import datetime

# Inicializa o cliente S3 fora do handler para reutilização
s3 = boto3.client('s3', region_name='sa-east-1')

def lambda_handler(event, context):
    """
    Função Lambda para processar mensagens da fila SQS de pedidos
    e salvá-las no S3.
    """
    print(f"Recebido evento com {len(event['Records'])} mensagem(ns).")
    
    # Pega as variáveis de ambiente, com valores padrão para segurança
    bucket_name = os.environ.get('BUCKET_NAME')
    pasta_destino = os.environ.get('PASTA_DESTINO', 'pedidos/')

    if not bucket_name:
        raise ValueError("Variável de ambiente BUCKET_NAME não configurada.")

    # O evento SQS pode conter múltiplas mensagens em um lote
    for record in event['Records']:
        try:
            # O corpo da mensagem é uma string, precisa ser convertido para JSON
            corpo = json.loads(record['body'])
            print(f"Processando pedido: {corpo}")

            # Gera um nome de arquivo único
            id_mensagem = record['messageId']
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            nome_arquivo = f"{pasta_destino}pedido_{timestamp}_{id_mensagem}.json"

            # Envia o objeto para o S3
            s3.put_object(
                Bucket=bucket_name,
                Key=nome_arquivo,
                Body=json.dumps(corpo),
                ContentType='application/json'
            )

            print(f"Pedido salvo com sucesso no S3: s3://{bucket_name}/{nome_arquivo}")

        except Exception as e:
            print(f"Erro ao processar a mensagem: {e}")
            # Se ocorrer um erro, a mensagem não será removida da fila e
            # poderá ser processada novamente após o tempo de visibilidade.
            # É importante configurar uma Dead-Letter Queue (DLQ) no SQS.
            raise e

    return {
        'statusCode': 200,
        'body': json.dumps('Mensagens processadas com sucesso!')
    }