# Persistência local de cada nó.
#
# "Todos os nós do cluster (líder e réplicas) devem persistir:
#  1. O Termo atual e o Voto concedido (voted_for);
#  2. O log de operações (entradas com termo e índice);
#  3. O estado do banco final (dados comitados)."
#
# Cada nó grava três arquivos JSON em data/node<id>/:
#  estado.json  -> termo, voted_for, índice de commit e índice aplicado;
#  log.json     -> entradas do log (índice, termo, operação, chave e valor);
#  banco.json   -> dados comitados e visíveis para leitura.

import json
import os


def carregar_json(caminho, padrao):
    if not os.path.exists(caminho):
        return padrao
    with open(caminho, "r", encoding="utf-8") as arquivo:
        return json.load(arquivo)


def salvar_json(caminho, dados):
    pasta = os.path.dirname(caminho)
    os.makedirs(pasta, exist_ok=True)
    temporario = caminho + ".tmp"
    with open(temporario, "w", encoding="utf-8") as arquivo:
        json.dump(dados, arquivo, ensure_ascii=False, indent=2)
        arquivo.flush()
        os.fsync(arquivo.fileno())
    os.replace(temporario, caminho)
