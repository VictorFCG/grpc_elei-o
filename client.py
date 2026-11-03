# Cliente do sistema de replicação.
#
# "Cliente (0,30)
#  • Gravação com Redirecionamento: envia solicitações de escrita ao líder. Se tentar
#    gravar em um seguidor ou candidato, ele receberá a indicação de quem é o líder atual.
#  • Consulta nos nós do cluster: permite realizar leituras de dados em qualquer um
#    dos nós do cluster."
#
# "Consulta de Dados Comitados (Leitura Eventual)"
#
# O cliente também consulta o Status de todos os nós para exibir papel, termo e
# índice de commit, conforme exigido na execução da aplicação.

import argparse
import sys

import grpc

import raft_pb2
import raft_pb2_grpc
from config import NOS, TIMEOUT_ESCRITA, TIMEOUT_RPC


def conectar():
    stubs = {}
    canais = []
    for id_node, dados in NOS.items():
        canal = grpc.insecure_channel(f"{dados['host']}:{dados['porta']}")
        canais.append(canal)
        stubs[id_node] = raft_pb2_grpc.ServicoClienteStub(canal)
    return stubs, canais


def consultar_status(stubs, id_node):
    try:
        return stubs[id_node].Status(raft_pb2.RequisicaoStatus(), timeout=TIMEOUT_RPC)
    except grpc.RpcError:
        return None


def encontrar_lider(stubs):
    for id_node in NOS:
        status = consultar_status(stubs, id_node)
        if status is not None and status.papel == "lider":
            return id_node
    return None


def mostrar_status_cluster(stubs):
    print("\nNó | papel     | termo | commit | log | líder")
    print("---+-----------+-------+--------+-----+------")
    for id_node in NOS:
        status = consultar_status(stubs, id_node)
        if status is None:
            print(f"{id_node:>2} | indisponível")
            continue
        lider = status.id_lider if status.id_lider else "-"
        print(
            f"{status.id:>2} | {status.papel:<9} | {status.termo:>5} | "
            f"{status.indice_commit:>6} | {status.tamanho_log:>3} | {lider:>4}"
        )


def gravar(stubs, node_atual):
    chave = input("Chave: ").strip()
    valor = input("Valor: ").strip()
    if not chave:
        print("A chave não pode ser vazia.")
        return node_atual

    tentativas = 0
    while tentativas < 4:
        try:
            resposta = stubs[node_atual].Write(
                raft_pb2.RequisicaoEscrita(chave=chave, valor=valor),
                timeout=TIMEOUT_ESCRITA + 2,
            )
        except grpc.RpcError as erro:
            print(f"Falha de comunicação com o nó {node_atual}: {erro.code().name}")
            return node_atual

        if resposta.sucesso:
            print(
                f"OK: '{chave}' = '{valor}' comitado no índice {resposta.indice} "
                f"(termo {resposta.termo})"
            )
            return node_atual

        if resposta.id_lider and resposta.id_lider != node_atual:
            print(
                f"O nó {node_atual} não é líder (termo {resposta.termo}). "
                f"Líder atual: nó {resposta.id_lider}."
            )
            print(f"Redirecionando a escrita para o nó {resposta.id_lider}...")
            node_atual = resposta.id_lider
            tentativas += 1
            continue

        if not resposta.id_lider:
            lider = encontrar_lider(stubs)
            if lider is not None and lider != node_atual:
                print(f"Líder descoberto no cluster: nó {lider}. Redirecionando...")
                node_atual = lider
                tentativas += 1
                continue

        print(
            f"Escrita rejeitada pelo nó {node_atual}: {resposta.mensagem} "
            f"(termo {resposta.termo})"
        )
        return node_atual

    print("Não foi possível concluir a escrita após várias tentativas.")
    return node_atual


def consultar(stubs, node_atual):
    chave = input("Chave: ").strip()
    try:
        resposta = stubs[node_atual].Read(
            raft_pb2.RequisicaoLeitura(chave=chave), timeout=TIMEOUT_RPC
        )
    except grpc.RpcError as erro:
        print(f"Falha de comunicação com o nó {node_atual}: {erro.code().name}")
        return

    if resposta.encontrado:
        print(f"'{chave}' = '{resposta.valor}'")
    else:
        print(f"'{chave}' não encontrada (somente dados comitados são visíveis)")
    lider = resposta.id_lider if resposta.id_lider else "-"
    print(
        f"Lido do nó {node_atual} | papel={resposta.papel} | termo={resposta.termo} | "
        f"commit={resposta.indice_commit} | líder={lider}"
    )


def trocar_node(node_atual):
    entrada = input(f"Nó (1 a {len(NOS)}), atual {node_atual}: ").strip()
    try:
        id_node = int(entrada)
    except ValueError:
        print("Número inválido.")
        return node_atual
    if id_node not in NOS:
        print("Nó inexistente.")
        return node_atual
    return id_node


def main():
    parser = argparse.ArgumentParser(description="Cliente do cluster")
    parser.add_argument("node", nargs="?", type=int, default=1, choices=list(NOS.keys()))
    argumentos = parser.parse_args()
    node_atual = argumentos.node

    stubs, canais = conectar()
    try:
        while True:
            print()
            print(
                f"Cliente conectado ao nó {node_atual} "
                f"({NOS[node_atual]['host']}:{NOS[node_atual]['porta']})"
            )
            print("1 - Gravar chave/valor")
            print("2 - Consultar chave")
            print("3 - Status do cluster")
            print("4 - Trocar de nó")
            print("0 - Sair")
            opcao = input("Opção: ").strip()

            if opcao == "1":
                node_atual = gravar(stubs, node_atual)
            elif opcao == "2":
                consultar(stubs, node_atual)
            elif opcao == "3":
                mostrar_status_cluster(stubs)
            elif opcao == "4":
                node_atual = trocar_node(node_atual)
            elif opcao == "0":
                break
            else:
                print("Opção inválida.")
    except (KeyboardInterrupt, EOFError):
        print("\nCliente encerrando...")
    finally:
        for canal in canais:
            canal.close()


if __name__ == "__main__":
    sys.exit(main())
