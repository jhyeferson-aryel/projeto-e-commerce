# Projeto de Pipeline de Dados de E-commerce na AWS

Este projeto simula um pipeline de dados (ETL) completo para pedidos de um e-commerce, utilizando uma arquitetura serverless na AWS.

## Descrição

O objetivo é demonstrar de forma prática o fluxo de um evento de "pedido" desde sua ingestão até estar pronto para análise, passando pelas fases de Extração, Transformação e Carga.

## Arquitetura do Pipeline

O fluxo de dados segue as seguintes etapas:

1.  **Ingestão (`main.py`):** Um script Python simula a geração de pedidos e os envia para uma fila no Amazon SQS.
2.  **Extração (AWS Lambda):** Uma função Lambda é acionada por novas mensagens na fila SQS, lendo os pedidos e salvando os dados brutos em formato JSON no Amazon S3 (zona `raw`).
3.  **Transformação (AWS Glue):** Um Job do AWS Glue é executado para ler os dados JSON da zona `raw`, transformá-los (calculando o valor total) e convertê-los para o formato colunar Apache Parquet.
4.  **Carga (Amazon S3):** Os dados transformados são salvos de volta no S3, em uma "zona processada" (`processed`).
5.  **Análise (Amazon Athena & QuickSight):** Os dados processados são catalogados e podem ser consultados via SQL com o Athena e visualizados com o QuickSight.

## Como Executar

### Pré-requisitos

* Conta na AWS com as credenciais configuradas localmente.
* Python 3.8+.
* Infraestrutura AWS previamente criada (Fila SQS, Bucket S3, Função Lambda, etc.).

### Instalação

1.  Clone o repositório:
    ```bash
    git clone <url-do-seu-repositorio>
    cd <nome-do-repositorio>
    ```

2.  Instale as dependências:
    ```bash
    pip install -r requirements.txt
    ```

### Execução

Para iniciar o pipeline enviando os dados de teste para a fila SQS, execute:
```bash
python main.py
```