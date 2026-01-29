# MWFaaS: Um Framework Mestre-Escravo para Execução de Funções como Serviço

MWFaaS (Master-Worker Framework for FaaS) é um framework em Python que simplifica a execução de tarefas paralelas em ambientes de Função como Serviço (FaaS), utilizando o paradigma mestre-escravo. Ele foi projetado para ser extensível, permitindo que os desenvolvedores integrem facilmente diferentes provedores de nuvem e estratégias de distribuição de dados.

## Features

*   **Arquitetura Mestre-Escravo:** Orquestra a execução de tarefas em múltiplos workers.
*   **Abstração de Nuvem:** A classe `CloudManager` permite a integração com diferentes provedores FaaS.
*   **Estratégias de Distribuição de Dados:** A classe `DistributionStrategy` permite a implementação de diferentes estratégias de divisão de dados.
*   **Serialização de Funções:** Utiliza `cloudpickle` para serializar e desserializar funções e seus dados, permitindo a execução remota.
*   **Interface de Linha de Comando (CLI):** Fornece ferramentas para autenticação e gerenciamento de endpoints.

## Arquitetura

O MWFaaS é composto por três componentes principais:

1.  **Master:** O ponto de entrada do framework. O `Master` é responsável por:
    *   Aceitar os dados de entrada e a função a ser executada.
    *   Utilizar uma `DistributionStrategy` para dividir os dados em pedaços (chunks).
    *   Utilizar um `CloudManager` para submeter os chunks de dados como tarefas para os workers.
    *   Coletar e agregar os resultados dos workers.

2.  **CloudManager:** Uma classe abstrata que define a interface para interagir com um provedor FaaS. As implementações concretas do `CloudManager` são responsáveis por:
    *   Gerenciar a autenticação com o provedor de nuvem.
    *   Listar os workers (endpoints) disponíveis.
    *   Submeter tarefas para os workers.

3.  **DistributionStrategy:** Uma classe abstrata que define a interface para dividir os dados de entrada em chunks. As implementações concretas da `DistributionStrategy` permitem diferentes estratégias de paralelização.

## Getting Started

### Pré-requisitos

*   Python 3.8 ou superior
*   As dependências listadas no arquivo `requirements.txt`

### Instalação

1.  Clone o repositório:
    ```bash
    git clone https://github.com/seu-usuario/mwfaas.git
    cd mwfaas
    ```

2.  Instale as dependências:
    ```bash
    pip install -r requirements.txt
    ```

### Configuração do Globus Compute

O MWFaaS inclui uma implementação de `CloudManager` para o Globus Compute. Para utilizá-lo, você precisa configurar seus endpoints do Globus Compute.

1.  **Login no Globus:**
    ```bash
    python3 -m mwfaas.cli.auth_globus login
    ```
    Este comando irá abrir uma página de autenticação no seu navegador.

2.  **Configure os Endpoints:**
    ```bash
    python3 -m mwfaas.cli.configure_globus_endpoints
    ```
    Este comando irá listar seus endpoints disponíveis e permitir que você selecione quais deseja usar com o MWFaaS. A configuração será salva em um arquivo `globus_config.json`.

## Usage

O exemplo a seguir demonstra como usar o MWFaaS para calcular o quadrado de uma lista de números em paralelo.

```python
from mwfaas.master import Master
from mwfaas.globus_compute_manager import GlobusComputeCloudManager
from mwfaas.list_distribution_strategy import ListDistributionStrategy

# 1. Defina a função que será executada pelos workers
def square(data_chunk, metadata=None):
    return [x * x for x in data_chunk]

# 2. Defina a função de redução para agregar os resultados
def aggregate_results(results):
    return [item for sublist in results for item in sublist]

# 3. Inicialize os componentes do MWFaaS
cloud_manager = GlobusComputeCloudManager()
distribution_strategy = ListDistributionStrategy(items_per_chunk=2)
master = Master(cloud_manager, distribution_strategy)

# 4. Execute as tarefas
data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
results = master.run(data, square)

# 5. Reduza os resultados
final_result = master.reduce(results, aggregate_results)

print(f"Resultado final: {final_result}")
```

## CLI

O MWFaaS fornece uma CLI para gerenciar a autenticação e os endpoints do Globus Compute.

*   **Login:**
    ```bash
    python3 -m mwfaas.cli.auth_globus login
    ```

*   **Logout:**
    ```bash
    python3 -m mwfaas.cli.auth_globus logout
    ```

*   **Configurar Endpoints:**
    ```bash
    python3 -m mwfaas.cli.configure_globus_endpoints
    ```

*   **Listar Endpoints:**
    ```bash
    python3 -m mwfaas.cli.list_globus_endpoints
    ```
    Este comando lista os endpoints do Globus Compute disponíveis e seu status. A coluna "CONFIGURADO" indica se o endpoint foi selecionado durante a etapa de configuração e está pronto para ser usado pelo MWFaaS.
