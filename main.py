import json
from datetime import datetime
from src.services.sqs import enviar_pedido
import time

# Carrega os pedidos do arquivo JSON
try:
    with open('pedidos.json', 'r', encoding='utf-8') as f:
        lista_pedidos = json.load(f)
except FileNotFoundError:
    print("Erro: Arquivo 'pedidos.json' não encontrado.")
    exit()

print(f"Encontrados {len(lista_pedidos)} pedidos para enviar.")

# Itera sobre a lista e envia cada pedido
for pedido in lista_pedidos:
    # Adiciona a data e hora do envio ao pedido
    pedido['data_pedido'] = datetime.now().isoformat()
    
    try:
        enviar_pedido(pedido)
        print(f"Pedido do usuário '{pedido['usuario']}' para o produto '{pedido['produto']}' enviado com sucesso.")
    except Exception as e:
        print(f"Erro ao enviar pedido: {e}")
        
    # Uma pequena pausa para não sobrecarregar a API da AWS em testes
    time.sleep(1)

print("\nTodos os pedidos foram enviados.")