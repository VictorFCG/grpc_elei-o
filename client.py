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
from config import NOS, TIMEOUT_RPC


def conectar():
    stubs = {}
    canais = []
    for id_no, dados in NOS.items():
        canal = grpc.insecure_channel(f"{dados['host']}:{dados['porta']}")
        canais.append(canal)
        stubs[id_no] = raft_pb2_grpc.ServicoClienteStub(canal)
    return stubs, canais


def consultar_status(stubs, id_no):
    try:
        return stubs[id_no].Status(raft_pb2.RequisicaoStatus(), timeout=TIMEOUT_RPC)
    except grpc.RpcError:
        return None


def encontrar_lider(stubs):
    for id_no in NOS:
        status = consultar_status(stubs, id_no)
        if status is not None and status.papel == "lider":
            return id_no
    return None


def mostrar_status_cluster(stubs):
    print("\nNó | papel     | termo | commit | log | líder")
    print("---+-----------+-------+--------+-----+------")
    for id_no in NOS:
        status = consultar_status(stubs, id_no)
        if status is None:
            print(f"{id_no:>2} | indisponível")
            continue
        lider = status.id_lider if status.id_lider else "-"
        print(
            f"{status.id:>2} | {status.papel:<9} | {status.termo:>5} | "
            f"{status.indice_commit:>6} | {status.tamanho_log:>3} | {lider:>4}"
        )


def gravar(stubs, no_atual):
    chave = input("Chave: ").strip()
    valor = input("Valor: ").strip()
    if not chave:
        print("A chave não pode ser vazia.")
        return no_atual

    tentativas = 0
    while tentativas < 4:
        try:
            resposta = stubs[no_atual].Write(
                raft_pb2.RequisicaoEscrita(chave=chave, valor=valor),
                timeout=TIMEOUT_RPC + 6.0,
            )
        except grpc.RpcError as erro:
            print(f"Falha de comunicação com o nó {no_atual}: {erro.code().name}")
            return no_atual

        if resposta.sucesso:
            print(
                f"OK: '{chave}' = '{valor}' comitado no índice {resposta.indice} "
                f"(termo {resposta.termo})"
            )
            return no_atual

        if resposta.id_lider and resposta.id_lider != no_atual:
            print(
                f"O nó {no_atual} não é líder (termo {resposta.termo}). "
                f"Líder atual: nó {resposta.id_lider}."
            )
            print(f"Redirecionando a escrita para o nó {resposta.id_lider}...")
            no_atual = resposta.id_lider
            tentativas += 1
            continue

        if not resposta.id_lider:
            lider = encontrar_lider(stubs)
            if lider is not None and lider != no_atual:
                print(f"Líder descoberto no cluster: nó {lider}. Redirecionando...")
                no_atual = lider
                tentativas += 1
                continue

        print(
            f"Escrita rejeitada pelo nó {no_atual}: {resposta.mensagem} "
            f"(termo {resposta.termo})"
        )
        return no_atual

    print("Não foi possível concluir a escrita após várias tentativas.")
    return no_atual


def consultar(stubs, no_atual):
    chave = input("Chave: ").strip()
    try:
        resposta = stubs[no_atual].Read(
            raft_pb2.RequisicaoLeitura(chave=chave), timeout=TIMEOUT_RPC
        )
    except grpc.RpcError as erro:
        print(f"Falha de comunicação com o nó {no_atual}: {erro.code().name}")
        return

    if resposta.encontrado:
        print(f"'{chave}' = '{resposta.valor}'")
    else:
        print(f"'{chave}' não encontrada (somente dados comitados são visíveis)")
    lider = resposta.id_lider if resposta.id_lider else "-"
    print(
        f"Lido do nó {no_atual} | papel={resposta.papel} | termo={resposta.termo} | "
        f"commit={resposta.indice_commit} | líder={lider}"
    )


def trocar_no(no_atual):
    entrada = input(f"Nó (1 a {len(NOS)}), atual {no_atual}: ").strip()
    try:
        id_no = int(entrada)
    except ValueError:
        print("Número inválido.")
        return no_atual
    if id_no not in NOS:
        print("Nó inexistente.")
        return no_atual
    return id_no


def main():
    parser = argparse.ArgumentParser(description="Cliente do cluster")
    parser.add_argument("no", nargs="?", type=int, default=1, choices=list(NOS.keys()))
    argumentos = parser.parse_args()
    no_atual = argumentos.no

    stubs, canais = conectar()
    try:
        while True:
            print()
            print(
                f"Cliente conectado ao nó {no_atual} "
                f"({NOS[no_atual]['host']}:{NOS[no_atual]['porta']})"
            )
            print("1 - Gravar chave/valor")
            print("2 - Consultar chave")
            print("3 - Status do cluster")
            print("4 - Trocar de nó")
            print("0 - Sair")
            opcao = input("Opção: ").strip()

            if opcao == "1":
                no_atual = gravar(stubs, no_atual)
            elif opcao == "2":
                consultar(stubs, no_atual)
            elif opcao == "3":
                mostrar_status_cluster(stubs)
            elif opcao == "4":
                no_atual = trocar_no(no_atual)
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
