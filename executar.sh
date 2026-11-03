#!/usr/bin/env bash

# Sobe o cluster de 4 nós e o cliente, cada processo em uma aba do tmux.
#
# "O ambiente é composto por 5 processos distintos:
#  • 1 Cliente
#  • 4 nós do Cluster (N = 4). Estes nós possuem o mesmo código e transitam
#    dinamicamente entre os papéis: Seguidor, Candidato e Líder."
#
# Uso:
#   ./executar.sh              inicia (ou anexa) as 5 abas
#   ./executar.sh node <id>    inicia um nó no terminal atual
#   ./executar.sh client [id]  inicia o cliente no terminal atual
#   ./executar.sh parar        encerra a sessão e os processos
#   ./executar.sh limpar       apaga os dados persistidos
#   ./executar.sh proto        regenera o código gRPC

DIRETORIO="$(cd "$(dirname "$0")" && pwd)"
SESSAO="cluster_grpc"

if ! command -v tmux >/dev/null 2>&1; then
    echo "tmux não encontrado. Instale com: sudo apt install tmux"
    exit 1
fi

anexar() {
    if [ -t 1 ]; then
        exec tmux attach -t "$SESSAO"
    fi
    echo "Sessão '$SESSAO' pronta. Para entrar: tmux attach -t $SESSAO"
}

case "${1:-iniciar}" in
    iniciar)
        if tmux has-session -t "$SESSAO" 2>/dev/null; then
            anexar
            exit 0
        fi

        tmux new-session -d -s "$SESSAO" -n "Nó 1" -c "$DIRETORIO"
        for id_node in 2 3 4; do
            tmux new-window -t "$SESSAO" -n "Nó $id_node" -c "$DIRETORIO"
        done
        tmux new-window -t "$SESSAO" -n "Cliente" -c "$DIRETORIO"

        sleep 1
        tmux send-keys -t "$SESSAO:Nó 1" "python3 node.py 1" C-m
        for id_node in 2 3 4; do
            tmux send-keys -t "$SESSAO:Nó $id_node" "python3 node.py $id_node" C-m
        done
        tmux send-keys -t "$SESSAO:Cliente" "python3 client.py 1" C-m

        tmux set-option -t "$SESSAO" mouse on
        tmux select-window -t "$SESSAO:Cliente"
        anexar
        ;;
    node)
        cd "$DIRETORIO"
        exec python3 node.py "${2:?informe o id do nó (1 a 4)}"
        ;;
    client)
        cd "$DIRETORIO"
        exec python3 client.py "${2:-1}"
        ;;
    parar)
        tmux kill-session -t "$SESSAO" 2>/dev/null || echo "Sessão '$SESSAO' não está rodando."
        ;;
    limpar)
        if tmux has-session -t "$SESSAO" 2>/dev/null; then
            echo "Pare a sessão antes de limpar: ./executar.sh parar"
            exit 1
        fi
        rm -rf "$DIRETORIO/data"
        echo "Dados apagados."
        ;;
    proto)
        cd "$DIRETORIO"
        exec python3 -m grpc_tools.protoc -Iproto --python_out=. --grpc_python_out=. proto/raft.proto
        ;;
    *)
        echo "Uso: ./executar.sh [iniciar|node <id>|client [id]|parar|limpar|proto]"
        exit 1
        ;;
esac
