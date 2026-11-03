# Configuração comum ao cluster e ao cliente.
#
# "O ambiente é composto por 5 processos distintos:
#  • 1 Cliente
#  • 4 nós do Cluster (N = 4). Estes nós possuem o mesmo código e transitam
#    dinamicamente entre os papéis: Seguidor, Candidato e Líder."
# "Quórum fixo: ... Para este cluster de 4 nós, o quórum é de 3 nós."
# "as réplicas enviam requisições periódicas de FetchRequest ao líder (ex: a cada 100ms)"

NOS = {
    1: {"host": "localhost", "porta": 50051},
    2: {"host": "localhost", "porta": 50052},
    3: {"host": "localhost", "porta": 50053},
    4: {"host": "localhost", "porta": 50054},
}

QUORUM = 3

INTERVALO_FETCH = 0.1
INTERVALO_LACO = 0.05
HEARTBEAT_TIMEOUT = 1.5

TIMEOUT_ELEICAO_MIN = 0.9
TIMEOUT_ELEICAO_MAX = 1.5

TIMEOUT_ESCRITA = 4.0
TIMEOUT_RPC = 0.4

PASTA_DADOS = "data"
