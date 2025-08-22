import json
from datetime import datetime
from src.services.sqs import enviar_pedido
import time
import logging
import argparse

# Configuração básica do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def carregar_pedidos(caminho_arquivo):
    """Carrega os pedidos de um arquivo JSON."""
    try:
        with open(caminho_arquivo, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"Arquivo '{caminho_arquivo}' não encontrado.")
        return None
    except json.JSONDecodeError:
        logging.error(f"Erro ao decodificar o JSON do arquivo '{caminho_arquivo}'.")
        return None

def processar_pedidos(lista_pedidos):
    """Itera sobre a lista de pedidos e envia cada um."""
    if not lista_pedidos:
        logging.warning("Nenhum pedido para processar.")
        return

    logging.info(f"Encontrados {len(lista_pedidos)} pedidos para enviar.")
    for pedido in lista_pedidos:
        pedido['data_pedido'] = datetime.now().isoformat()
        try:
            enviar_pedido(pedido)
            logging.info(f"Pedido do usuário '{pedido.get('usuario', 'N/A')}' para o produto '{pedido.get('produto', 'N/A')}' enviado com sucesso.")
        except Exception as e:
            logging.error(f"Erro ao enviar pedido: {e}")
        time.sleep(1) # Pausa para não sobrecarregar a API
    logging.info("Todos os pedidos foram processados.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Processador de pedidos de e-commerce.")
    parser.add_argument("arquivo_pedidos", default="pedidos.json", nargs="?", help="Caminho para o arquivo JSON de pedidos. Padrão: pedidos.json")
    args = parser.parse_args()

    pedidos = carregar_pedidos(args.arquivo_pedidos)
    if pedidos:
        processar_pedidos(pedidos)