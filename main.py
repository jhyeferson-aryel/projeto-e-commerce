from datetime import datetime
from src.services.sqs import enviar_pedido

pedido = {
    'usuario': 'Bruno',
    'produto': 'Teclado Mecânico',
    'quantidade': 1,
    'valor_unitario': 379.90,
    'data_pedido': datetime.now().isoformat()
}

enviar_pedido(pedido)
print("Pedido enviado com sucesso.")
