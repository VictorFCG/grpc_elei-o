# Nó do cluster (seguidor, candidato ou líder).
#
# "O ambiente é composto por ... 4 nós do Cluster (N = 4). Estes nós possuem o
# mesmo código e transitam dinamicamente entre os papéis: Seguidor, Candidato e Líder."
# "A comunicação entre todos os nós deve ser realizada exclusivamente via gRPC."
#
# "Incremento do Termo: quando um nó não recebe comunicação do líder por um tempo
# (election timeout), ele se torna Candidato e incrementa o seu termo."
# "Atualização Obrigatória de Termo: se qualquer nó (incluindo o líder) receber uma
# mensagem ou resposta contendo um Termo estritamente maior que o seu termo local
# atual, ele deve imediatamente atualizar seu termo para esse novo valor e se
# transformar em Seguidor."
# "Voto Único: um nó só pode votar em no máximo um candidato por Termo. Um nó só
# concede seu voto a um candidato se o log do candidato for tão ou mais atualizado
# que o seu próprio log."
#
# "FetchRequest como Heartbeat: as réplicas enviam requisições periódicas de
# FetchRequest ao líder (ex: a cada 100ms) informando seu último termo e último índice."
# "Temporizador do líder: o líder mantém um Heartbeat Timeout individual para cada
# réplica. Se uma réplica parar de enviar FetchRequests, o líder a considera inativa."
# "Índice de commit: o maior índice do log que já foi replicado e armazenado na
# maioria dos nós (quórum)."
#
# "Sem quórum, sem escrita: se o líder não conseguir confirmar a replicação no quórum
# ... a operação não pode ser comitada e a escrita do cliente fica bloqueada/rejeitada."
# "Truncamento e Correção de Log: ao detectar divergências de log, o nó trunca o log
# local a partir do ponto conflitante e ressincroniza com o líder atual."

import argparse
import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import grpc

import raft_pb2
import raft_pb2_grpc
from config import (
    HEARTBEAT_TIMEOUT,
    INTERVALO_FETCH,
    INTERVALO_LACO,
    NOS,
    PASTA_DADOS,
    QUORUM,
    TIMEOUT_ELEICAO_MAX,
    TIMEOUT_ELEICAO_MIN,
    TIMEOUT_ESCRITA,
    TIMEOUT_RPC,
)
from storage import carregar_json, salvar_json


class Node(raft_pb2_grpc.ServicoRaftServicer, raft_pb2_grpc.ServicoClienteServicer):
    def __init__(self, id_no):
        self.id = id_no
        self.cond = threading.Condition()
        self.papel = "seguidor"
        self.termo = 0
        self.voted_for = 0
        self.id_lider = None
        self.log = []
        self.indice_commit = 0
        self.indice_aplicado = 0
        self.banco = {}

        self.ultimo_contato_lider = time.monotonic()
        self.inicio_eleicao = 0.0
        self.timeout_eleicao = self.sortear_timeout()
        self.ultimo_fetch = {id_no: 0.0 for id_no in NOS}
        self.match = {id_no: 0 for id_no in NOS}
        self.ultimo_envio_fetch = 0.0
        self.ultima_impressao = 0.0
        self.parar = False

        self.pasta = os.path.join(PASTA_DADOS, f"node{id_no}")
        self.caminho_estado = os.path.join(self.pasta, "estado.json")
        self.caminho_log = os.path.join(self.pasta, "log.json")
        self.caminho_banco = os.path.join(self.pasta, "banco.json")
        self.carregar()

        self.canais = []
        self.stubs = self.criar_stubs()

    def sortear_timeout(self):
        return random.uniform(TIMEOUT_ELEICAO_MIN, TIMEOUT_ELEICAO_MAX)

    def criar_stubs(self):
        stubs = {}
        for id_no, dados in NOS.items():
            if id_no == self.id:
                continue
            canal = grpc.insecure_channel(f"{dados['host']}:{dados['porta']}")
            self.canais.append(canal)
            stubs[id_no] = raft_pb2_grpc.ServicoRaftStub(canal)
        return stubs

    def carregar(self):
        estado = carregar_json(self.caminho_estado, {})
        self.termo = estado.get("termo", 0)
        self.voted_for = estado.get("voted_for", 0)
        self.indice_commit = estado.get("indice_commit", 0)
        self.indice_aplicado = estado.get("indice_aplicado", 0)
        self.log = carregar_json(self.caminho_log, [])
        self.banco = carregar_json(self.caminho_banco, {})
        if self.indice_commit > len(self.log):
            self.indice_commit = len(self.log)
        if self.indice_aplicado > self.indice_commit:
            self.indice_aplicado = self.indice_commit
        self.aplicar_commits()

    def salvar_estado(self):
        salvar_json(
            self.caminho_estado,
            {
                "termo": self.termo,
                "voted_for": self.voted_for,
                "indice_commit": self.indice_commit,
                "indice_aplicado": self.indice_aplicado,
            },
        )

    def salvar_log(self):
        salvar_json(self.caminho_log, self.log)

    def imprimir_status(self, motivo=None):
        with self.cond:
            lider = self.id_lider if self.id_lider else "-"
            linha = (
                f"[{time.strftime('%H:%M:%S')}] Nó {self.id} | "
                f"papel={self.papel} | termo={self.termo} | "
                f"commit={self.indice_commit} | log={len(self.log)} | líder={lider}"
            )
            if self.papel == "lider":
                linha += f" | ativas={self.replicas_ativas()}"
        if motivo:
            linha = f"{linha} | {motivo}"
        print(linha, flush=True)

    def imprimir_status_periodico(self):
        agora = time.monotonic()
        if agora - self.ultima_impressao >= 1.0:
            self.ultima_impressao = agora
            self.imprimir_status()

    def aplicar_commits(self):
        if self.indice_aplicado >= self.indice_commit:
            return
        while self.indice_aplicado < self.indice_commit:
            entrada = self.log[self.indice_aplicado]
            if entrada["operacao"] == "PUT":
                self.banco[entrada["chave"]] = entrada["valor"]
            self.indice_aplicado += 1
        salvar_json(self.caminho_banco, self.banco)
        self.salvar_estado()

    def atualizar_termo(self, novo_termo):
        if novo_termo <= self.termo:
            return
        self.termo = novo_termo
        self.voted_for = 0
        self.papel = "seguidor"
        self.id_lider = None
        self.ultimo_contato_lider = time.monotonic()
        self.timeout_eleicao = self.sortear_timeout()
        self.salvar_estado()
        self.cond.notify_all()
        self.imprimir_status("termo atualizado, virou seguidor")

    def ultimo_termo_log(self):
        if not self.log:
            return 0
        return self.log[-1]["termo"]

    def log_mais_atualizado(self, ultimo_indice, ultimo_termo):
        meu_indice = len(self.log)
        meu_termo = self.ultimo_termo_log()
        if ultimo_termo != meu_termo:
            return ultimo_termo > meu_termo
        return ultimo_indice >= meu_indice

    def replicas_ativas(self):
        agora = time.monotonic()
        ativas = [self.id]
        for id_no in NOS:
            if id_no == self.id:
                continue
            if agora - self.ultimo_fetch.get(id_no, 0.0) <= HEARTBEAT_TIMEOUT:
                ativas.append(id_no)
        return ativas

    def tem_quorum_ativo(self):
        return len(self.replicas_ativas()) >= QUORUM

    def calcular_commit(self):
        if self.papel != "lider":
            return
        for indice in range(len(self.log), self.indice_commit, -1):
            confirmacoes = 1
            for id_no in NOS:
                if id_no == self.id:
                    continue
                if self.match.get(id_no, 0) >= indice:
                    confirmacoes += 1
            if confirmacoes >= QUORUM:
                self.indice_commit = indice
                self.aplicar_commits()
                self.cond.notify_all()
                self.imprimir_status("commit avançou")
                return

    def RequestVote(self, requisicao, contexto):
        with self.cond:
            if requisicao.termo > self.termo:
                self.atualizar_termo(requisicao.termo)
            if requisicao.termo < self.termo:
                return raft_pb2.RespostaVoto(termo=self.termo, voto_concedido=False)

            pode_votar = self.voted_for in (0, requisicao.id_candidato)
            log_ok = self.log_mais_atualizado(requisicao.ultimo_indice, requisicao.ultimo_termo)
            conceder = pode_votar and log_ok
            if conceder:
                self.voted_for = requisicao.id_candidato
                self.ultimo_contato_lider = time.monotonic()
                self.timeout_eleicao = self.sortear_timeout()
                self.salvar_estado()
                print(
                    f"[{time.strftime('%H:%M:%S')}] Nó {self.id} concedeu voto ao "
                    f"nó {requisicao.id_candidato} no termo {self.termo}",
                    flush=True,
                )
            return raft_pb2.RespostaVoto(termo=self.termo, voto_concedido=conceder)

    def Fetch(self, requisicao, contexto):
        with self.cond:
            if requisicao.termo > self.termo:
                self.atualizar_termo(requisicao.termo)
            if requisicao.termo < self.termo:
                return raft_pb2.RespostaFetch(
                    termo=self.termo,
                    sucesso=False,
                    id_lider=self.id_lider or 0,
                    indice_commit=self.indice_commit,
                    ultimo_indice_lider=len(self.log),
                )
            if self.papel != "lider":
                return raft_pb2.RespostaFetch(
                    termo=self.termo,
                    sucesso=False,
                    id_lider=self.id_lider or 0,
                    indice_commit=self.indice_commit,
                    ultimo_indice_lider=len(self.log),
                )

            self.ultimo_fetch[requisicao.id_seguidor] = time.monotonic()
            ultimo_indice_lider = len(self.log)
            prefixo_confere = requisicao.ultimo_indice == 0 or (
                requisicao.ultimo_indice <= ultimo_indice_lider
                and self.log[requisicao.ultimo_indice - 1]["termo"] == requisicao.ultimo_termo
            )
            if prefixo_confere:
                self.match[requisicao.id_seguidor] = requisicao.ultimo_indice
                inicio = requisicao.ultimo_indice
            else:
                self.match[requisicao.id_seguidor] = 0
                inicio = 0

            entradas = []
            for entrada in self.log[inicio:]:
                entradas.append(
                    raft_pb2.EntradaLog(
                        indice=entrada["indice"],
                        termo=entrada["termo"],
                        operacao=entrada["operacao"],
                        chave=entrada["chave"],
                        valor=entrada["valor"],
                    )
                )

            self.calcular_commit()
            return raft_pb2.RespostaFetch(
                termo=self.termo,
                sucesso=True,
                id_lider=self.id,
                indice_commit=self.indice_commit,
                ultimo_indice_lider=ultimo_indice_lider,
                entradas=entradas,
            )

    def Write(self, requisicao, contexto):
        with self.cond:
            if self.papel != "lider":
                return raft_pb2.RespostaEscrita(
                    sucesso=False,
                    mensagem=f"nó {self.id} não é líder",
                    id_lider=self.id_lider or 0,
                    termo=self.termo,
                    indice=0,
                )
            if not self.tem_quorum_ativo():
                print(
                    f"[{time.strftime('%H:%M:%S')}] Nó {self.id} recusou escrita: "
                    "sem quórum ativo",
                    flush=True,
                )
                return raft_pb2.RespostaEscrita(
                    sucesso=False,
                    mensagem="sem quórum ativo: escrita bloqueada",
                    id_lider=self.id,
                    termo=self.termo,
                    indice=0,
                )

            entrada = {
                "indice": len(self.log) + 1,
                "termo": self.termo,
                "operacao": "PUT",
                "chave": requisicao.chave,
                "valor": requisicao.valor,
            }
            self.log.append(entrada)
            self.salvar_log()
            alvo = entrada["indice"]

            prazo = time.monotonic() + TIMEOUT_ESCRITA
            while (
                self.papel == "lider"
                and self.indice_commit < alvo
                and time.monotonic() < prazo
            ):
                self.cond.wait(0.05)

            if self.indice_commit >= alvo:
                print(
                    f"[{time.strftime('%H:%M:%S')}] Nó {self.id} comitou "
                    f"'{requisicao.chave}' no índice {alvo} (termo {self.termo})",
                    flush=True,
                )
                return raft_pb2.RespostaEscrita(
                    sucesso=True,
                    mensagem=f"gravado e comitado no índice {alvo}",
                    id_lider=self.id,
                    termo=self.termo,
                    indice=alvo,
                )

            print(
                f"[{time.strftime('%H:%M:%S')}] Nó {self.id} não comitou "
                f"'{requisicao.chave}' no índice {alvo}: quórum insuficiente",
                flush=True,
            )
            return raft_pb2.RespostaEscrita(
                sucesso=False,
                mensagem="entrada gravada no log, mas não comitada (quórum insuficiente)",
                id_lider=self.id,
                termo=self.termo,
                indice=alvo,
            )

    def Read(self, requisicao, contexto):
        with self.cond:
            encontrado = requisicao.chave in self.banco
            return raft_pb2.RespostaLeitura(
                encontrado=encontrado,
                valor=self.banco.get(requisicao.chave, ""),
                papel=self.papel,
                termo=self.termo,
                indice_commit=self.indice_commit,
                id_lider=self.id_lider or 0,
            )

    def Status(self, requisicao, contexto):
        with self.cond:
            return raft_pb2.RespostaStatus(
                id=self.id,
                papel=self.papel,
                termo=self.termo,
                indice_commit=self.indice_commit,
                tamanho_log=len(self.log),
                id_lider=self.id_lider or 0,
            )

    def processar_resposta_fetch(self, resposta):
        with self.cond:
            if resposta.termo > self.termo:
                self.atualizar_termo(resposta.termo)
            if resposta.termo < self.termo:
                return

            if not resposta.sucesso:
                if resposta.id_lider:
                    self.id_lider = resposta.id_lider
                return

            self.id_lider = resposta.id_lider
            self.ultimo_contato_lider = time.monotonic()
            if self.papel != "seguidor":
                self.papel = "seguidor"
                self.timeout_eleicao = self.sortear_timeout()
                self.imprimir_status("virou seguidor")

            entradas = []
            for entrada in resposta.entradas:
                entradas.append(
                    {
                        "indice": entrada.indice,
                        "termo": entrada.termo,
                        "operacao": entrada.operacao,
                        "chave": entrada.chave,
                        "valor": entrada.valor,
                    }
                )

            alterado = False
            for posicao, entrada in enumerate(entradas):
                if entrada["indice"] > len(self.log):
                    self.log.extend(entradas[posicao:])
                    alterado = True
                    break
                if self.log[entrada["indice"] - 1]["termo"] != entrada["termo"]:
                    print(
                        f"[{time.strftime('%H:%M:%S')}] Nó {self.id} truncou o log "
                        f"a partir do índice {entrada['indice']}",
                        flush=True,
                    )
                    self.log = self.log[: entrada["indice"] - 1]
                    self.log.extend(entradas[posicao:])
                    alterado = True
                    break

            if len(self.log) > resposta.ultimo_indice_lider:
                print(
                    f"[{time.strftime('%H:%M:%S')}] Nó {self.id} removeu entradas "
                    f"além do índice {resposta.ultimo_indice_lider}",
                    flush=True,
                )
                self.log = self.log[: resposta.ultimo_indice_lider]
                alterado = True

            if alterado:
                self.salvar_log()

            if resposta.indice_commit > self.indice_commit:
                self.indice_commit = min(resposta.indice_commit, len(self.log))
                self.aplicar_commits()
                self.imprimir_status("commit atualizado")

    def montar_requisicao_fetch(self):
        return raft_pb2.RequisicaoFetch(
            id_seguidor=self.id,
            termo=self.termo,
            ultimo_indice=len(self.log),
            ultimo_termo=self.ultimo_termo_log(),
        )

    def enviar_fetch(self):
        with self.cond:
            if self.papel == "lider":
                return
            id_lider = self.id_lider
            requisicao = self.montar_requisicao_fetch()

        if id_lider and id_lider != self.id:
            try:
                resposta = self.stubs[id_lider].Fetch(requisicao, timeout=TIMEOUT_RPC)
            except grpc.RpcError:
                resposta = None
            if resposta is not None:
                self.processar_resposta_fetch(resposta)
                if resposta.sucesso:
                    return
            with self.cond:
                if self.id_lider == id_lider:
                    self.id_lider = None

        for id_no in NOS:
            if id_no == self.id:
                continue
            with self.cond:
                requisicao = self.montar_requisicao_fetch()
            try:
                resposta = self.stubs[id_no].Fetch(requisicao, timeout=TIMEOUT_RPC)
            except grpc.RpcError:
                continue
            self.processar_resposta_fetch(resposta)
            if resposta.sucesso:
                return

    def descobrir_lider(self):
        with self.cond:
            if self.papel == "lider":
                return True
        for id_no in NOS:
            if id_no == self.id:
                continue
            with self.cond:
                requisicao = self.montar_requisicao_fetch()
            try:
                resposta = self.stubs[id_no].Fetch(requisicao, timeout=TIMEOUT_RPC)
            except grpc.RpcError:
                continue
            self.processar_resposta_fetch(resposta)
            if resposta.sucesso:
                return True
            if resposta.id_lider == id_no:
                return True
        return False

    def iniciar_eleicao(self):
        with self.cond:
            if self.papel == "lider":
                return
            self.papel = "candidato"
            self.termo += 1
            self.voted_for = self.id
            self.id_lider = None
            self.inicio_eleicao = time.monotonic()
            self.timeout_eleicao = self.sortear_timeout()
            self.salvar_estado()
            termo_eleicao = self.termo
            ultimo_indice = len(self.log)
            ultimo_termo = self.ultimo_termo_log()
            votos = 1
            self.imprimir_status("iniciou eleição")

        for id_no in NOS:
            if id_no == self.id:
                continue
            try:
                resposta = self.stubs[id_no].RequestVote(
                    raft_pb2.RequisicaoVoto(
                        id_candidato=self.id,
                        termo=termo_eleicao,
                        ultimo_indice=ultimo_indice,
                        ultimo_termo=ultimo_termo,
                    ),
                    timeout=TIMEOUT_RPC,
                )
            except grpc.RpcError:
                continue

            with self.cond:
                if resposta.termo > self.termo:
                    self.atualizar_termo(resposta.termo)
                    return
                if self.papel != "candidato" or self.termo != termo_eleicao:
                    return
                if resposta.voto_concedido:
                    votos += 1
                    if votos >= QUORUM:
                        self.virar_lider()
                        return

    def virar_lider(self):
        with self.cond:
            if self.papel != "candidato":
                return
            self.papel = "lider"
            self.id_lider = self.id
            self.match = {id_no: 0 for id_no in NOS}
            self.ultimo_fetch = {id_no: 0.0 for id_no in NOS}
            self.imprimir_status("virou líder")

    def laco_principal(self):
        while not self.parar:
            time.sleep(INTERVALO_LACO)
            with self.cond:
                papel = self.papel
                if papel == "seguidor":
                    venceu = time.monotonic() - self.ultimo_contato_lider >= self.timeout_eleicao
                elif papel == "candidato":
                    venceu = time.monotonic() - self.inicio_eleicao >= self.timeout_eleicao
                else:
                    venceu = False

            if papel == "lider":
                self.imprimir_status_periodico()
                continue

            if venceu:
                if not self.descobrir_lider():
                    self.iniciar_eleicao()
            else:
                agora = time.monotonic()
                if agora - self.ultimo_envio_fetch >= INTERVALO_FETCH:
                    self.ultimo_envio_fetch = agora
                    self.enviar_fetch()
            self.imprimir_status_periodico()

    def iniciar(self):
        thread = threading.Thread(target=self.laco_principal, daemon=True)
        thread.start()


def main():
    parser = argparse.ArgumentParser(description="Nó do cluster de replicação")
    parser.add_argument("id", type=int, choices=list(NOS.keys()))
    argumentos = parser.parse_args()

    node = Node(argumentos.id)
    servidor = grpc.server(ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_ServicoRaftServicer_to_server(node, servidor)
    raft_pb2_grpc.add_ServicoClienteServicer_to_server(node, servidor)

    porta = NOS[argumentos.id]["porta"]
    servidor.add_insecure_port(f"0.0.0.0:{porta}")
    servidor.start()
    node.iniciar()
    print(f"Nó {argumentos.id} escutando em 0.0.0.0:{porta}", flush=True)

    try:
        servidor.wait_for_termination()
    except KeyboardInterrupt:
        print(f"\nNó {argumentos.id} encerrando...", flush=True)
    finally:
        node.parar = True
        servidor.stop(0)
        for canal in node.canais:
            canal.close()


if __name__ == "__main__":
    main()
