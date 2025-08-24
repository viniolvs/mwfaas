# mwfaas/globus_compute_manager.py

import json
import cloudpickle
from typing import Any, Optional, List, Dict

from globus_compute_sdk import Client as GlobusComputeClient
from globus_compute_sdk import Executor
from concurrent.futures import Future
from .cloud_manager import CloudManager

DEFAULT_CONFIG_PATH = "globus_config.json"


class GlobusComputeCloudManager(CloudManager):
    """
    Um CloudManager para interagir com o Globus Compute.
    Ele lida com autenticação, seleção de endpoints, submissão de tarefas e coleta de resultados.
    """

    def __init__(
        self,
        endpoints_config: Optional[List[Dict[str, Any]]] = None,
        config_file_path: str = DEFAULT_CONFIG_PATH,
        auto_authenticate: bool = True,
    ):
        """
        Inicializa o GlobusComputeCloudManager.

        Args:
            endpoints_config: Uma lista opcional de dicionários, cada um descrevendo um endpoint.
            config_file_path: Caminho para o arquivo de configuração.
            auto_authenticate: Se True, tenta garantir a autenticação na inicialização.
        """
        self._config_file_path = config_file_path
        self._client = GlobusComputeClient()

        self.endpoints_details: List[Dict[str, Any]] = []
        self.active_endpoint_ids: List[str] = []
        self._executors: Dict[str, Executor] = {}

        if auto_authenticate:
            try:
                self._client.version_check()
            except Exception:
                print(
                    "Aviso: Autenticação com Globus Compute será solicitada na primeira operação."
                )

        loaded_config = []
        if endpoints_config is not None:
            loaded_config = endpoints_config
        else:
            loaded_config = self._load_config_from_file(self._config_file_path)
            if not loaded_config:
                print(
                    f"Nenhuma configuração de endpoint fornecida ou encontrada em '{self._config_file_path}'."
                )
                print(
                    "Execute o método 'configure_endpoints_interactive_and_save()' para começar."
                )

        if loaded_config:
            self._initialize_from_config(loaded_config)

    def get_active_worker_ids(self) -> List[str]:
        return self.active_endpoint_ids

    def _initialize_from_config(self, endpoints_config: List[Dict[str, Any]]):
        """Inicializa os atributos e executores a partir de uma configuração carregada."""
        self.shutdown_executors()  # Limpa executores antigos
        self.endpoints_details = endpoints_config
        endpoint_ids_to_init: List[str]
        endpoint_ids_to_init = [
            ep_id
            for ep in self.endpoints_details
            if (ep_id := ep.get("id")) and isinstance(ep_id, str)
        ]
        self._initialize_executors(endpoint_ids_to_init)

    def _initialize_executors(self, endpoint_ids_to_init: List[str]):
        self.active_endpoint_ids = []
        self._executors = {}
        for ep_id in endpoint_ids_to_init:
            try:
                status_info = self._client.get_endpoint_status(ep_id)
                if status_info.get("status") != "online":
                    print(
                        f"Aviso: Endpoint Globus Compute {ep_id} não está 'online' (status: {status_info.get('status')}). Não será utilizado."
                    )
                    continue
                self._executors[ep_id] = Executor(
                    endpoint_id=ep_id, client=self._client
                )
                self.active_endpoint_ids.append(ep_id)
            except Exception as e:
                print(
                    f"Aviso: Falha ao criar executor para o endpoint {ep_id}: {e}. Este endpoint não será utilizado."
                )

        if not self.active_endpoint_ids:
            print(
                "Aviso: Nenhum executor Globus Compute utilizável pôde ser inicializado."
            )

    def _load_config_from_file(self, config_path: str) -> List[Dict[str, Any]]:
        """Carrega a configuração de endpoints de um arquivo JSON."""
        try:
            with open(config_path, "r") as f:
                config = json.load(f)
                endpoints = config.get("globus_compute_endpoints", [])
                if isinstance(endpoints, list):
                    return endpoints
                else:
                    print(
                        f"Erro: Formato inválido para 'globus_compute_endpoints' em {config_path}. Esperava uma lista."
                    )
                    return []
        except FileNotFoundError:
            return []
        except json.JSONDecodeError:
            print(f"Erro: Não foi possível decodificar JSON de {config_path}.")
            return []
        except Exception as e:
            print(f"Erro inesperado ao carregar configuração de {config_path}: {e}")
            return []

    # --- Implementação dos Métodos Abstratos de CloudManager ---

    def get_worker_count(self) -> int:
        """
        Retorna o número de endpoints Globus Compute ativos e utilizáveis.
        Pode ser ajustado para refletir o total de workers, se essa informação estiver disponível.
        """
        if not self.active_endpoint_ids or not self._executors:
            return 0
        return len(self.active_endpoint_ids)

    def submit_task(
        self, worker_id: str, serialized_function_bytes: bytes, data_chunk: Any
    ) -> Future:
        """
        Submete uma tarefa a um dos endpoints Globus Compute configurados (usando round-robin).
        A função é desserializada antes da submissão, pois GlobusComputeExecutor espera um callable.
        """
        if not self._executors or not self.active_endpoint_ids:
            raise RuntimeError(
                "Nenhum executor Globus Compute está disponível/configurado para submissão de tarefas."
            )

        try:
            user_function = cloudpickle.loads(serialized_function_bytes)
        except Exception as e:
            raise ValueError(
                f"Falha ao desserializar a função do usuário para o Globus Compute: {e}"
            ) from e

        executor = self._executors[worker_id]
        if executor is None:
            raise RuntimeError(
                f"Executor Globus Compute para o endpoint {worker_id} não foi inicializado."
            )

        try:
            future = executor.submit(user_function, data_chunk)
            return future

        except Exception as e:
            raise RuntimeError(
                f"Falha ao submeter tarefa ao endpoint Globus Compute {worker_id}: {e}"
            ) from e

    def shutdown_executors(self):
        """Desliga todos os executores Globus Compute ativos."""
        if not self._executors:
            return
        for endpoint_id, executor in self._executors.items():
            try:
                executor.shutdown(wait=True)
            except Exception as e:
                print(f"Erro ao desligar o executor para o endpoint {endpoint_id}: {e}")
        self._executors.clear()

    def shutdown(self):
        """Método de limpeza para o CloudManager."""
        self.shutdown_executors()

    # --- Métodos de Configuração e Autenticação  ---

    @staticmethod
    def logout():
        """Realiza a desautenticação (logout) com o Globus Compute."""
        print("\n--- Logout Globus ---")
        try:
            client = GlobusComputeClient()
            client.logout()
            print("Desautenticação Globus bem-sucedida.")
        except Exception as e:
            print(f"Erro durante a desautenticação: {e}")

    @staticmethod
    def login_interactive() -> bool:
        """Realiza a autenticação interativa com o Globus Compute."""
        print("\n--- Login Globus ---")
        print(
            "Siga as instruções na tela/navegador para completar o processo de login."
        )
        try:
            client = GlobusComputeClient()
            client.version_check()
            print("Autenticação Globus foi bem-sucedida.")
            return True
        except Exception as e:
            print(f"Erro durante a tentativa de autenticação: {e}")
            print("Por favor, tente novamente ou verifique sua configuração do Globus.")
            return False

    @staticmethod
    def parse_endpoint_specs(ep_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extrai e organiza as especificações relevantes do metadata de um endpoint do Globus Compute.

        Args:
            ep_metadata: O dicionário completo de metadata retornado pela API do Globus Compute.

        Returns:
            Um dicionário contendo as especificações organizadas.
        """
        if not ep_metadata:
            return {}

        config = ep_metadata.get("config", {})
        engine = config.get("engine", {})
        executor = engine.get("executor", {})
        provider = executor.get("provider", {})

        specs = {
            # Identificação
            "hostname": ep_metadata.get("hostname"),
            "ip_address": ep_metadata.get("ip_address"),
            # Desempenho e Capacidade (extraído automaticamente)
            "execution_provider": provider.get("type"),
            "max_parallel_workers": executor.get("max_workers_per_node"),
            "cores_per_worker": executor.get("cores_per_worker"),
            "available_accelerators": executor.get("available_accelerators", []),
            # Software
            "python_version": ep_metadata.get("python_version"),
            "endpoint_version": ep_metadata.get("endpoint_version"),
        }
        return specs

    @staticmethod
    def select_endpoints_interactive() -> List[Dict[str, Any]]:
        """
        Permite ao usuário selecionar interativamente endpoints e adicionar especificações customizadas.
        Retorna uma lista de dicionários, cada um representando um endpoint configurado.
        """
        print("\n--- Seleção Interativa de Endpoints Globus Compute ---")
        client = GlobusComputeClient()
        print("Listando seus endpoints Globus Compute existentes...")

        available_endpoints = []
        try:
            for ep_info in client.get_endpoints():
                try:
                    status_info = client.get_endpoint_status(ep_info["uuid"])
                    available_endpoints.append(
                        {
                            "uuid": ep_info["uuid"],
                            "name": ep_info["name"],
                            "status": status_info["status"],
                        }
                    )
                except Exception as e_status:
                    print(
                        f"  Aviso: Não foi possível obter status para {ep_info.get('name')}: {e_status}"
                    )
        except Exception as e_list:
            print(f"Não foi possível listar os endpoints: {e_list}")
            return []

        if not available_endpoints:
            print("\nNenhum endpoint encontrado.")
            return []

        for i, details in enumerate(available_endpoints):
            print(
                f"  {i + 1} - Nome: {details['name']} (ID: {details['uuid']}) - Status: {details['status']}"
            )

        final_selected_endpoints = []
        while True:
            user_input = (
                input(
                    "\nDigite o número do endpoint que deseja configurar (ou 'q' para finalizar a seleção): "
                )
                .strip()
                .lower()
            )
            if user_input == "q":
                if final_selected_endpoints:
                    print(
                        f"\nSeleção finalizada. Endpoints configurados: {[ep['name'] for ep in final_selected_endpoints]}"
                    )
                else:
                    print("\nSeleção finalizada sem nenhum endpoint configurado.")
                return final_selected_endpoints

            try:
                num = int(user_input)
                index = num - 1
                if not (0 <= index < len(available_endpoints)):
                    print("Número inválido. Tente novamente.")
                    continue

                selected_ep_details = available_endpoints[index]

                # Verifica se o endpoint já foi adicionado
                if any(
                    ep["id"] == selected_ep_details["uuid"]
                    for ep in final_selected_endpoints
                ):
                    print(
                        f"O endpoint '{selected_ep_details['name']}' já foi adicionado."
                    )
                    continue

                # Adicionar especificações customizadas
                custom_specs = {}
                add_specs = (
                    input(
                        f"Deseja adicionar especificações para '{selected_ep_details['name']}'? (s/N): "
                    )
                    .strip()
                    .lower()
                )
                if add_specs == "s":
                    custom_specs = GlobusComputeCloudManager.parse_endpoint_specs(
                        client.get_endpoint_metadata(selected_ep_details["uuid"])
                    )
                    print(
                        "Digite as especificações no formato 'chave=valor'. Deixe em branco e pressione ENTER para finalizar."
                    )
                    while True:
                        spec_input = input("  Spec (ex: ram=16GB, tags=gpu): ").strip()
                        if not spec_input:
                            break
                        if "=" not in spec_input:
                            print("  Formato inválido. Use 'chave=valor'.")
                            continue
                        key, value = spec_input.split("=", 1)
                        custom_specs[key.strip()] = value.strip()

                endpoint_object = {
                    "id": selected_ep_details["uuid"],
                    "name": selected_ep_details["name"],
                    "specs": custom_specs,
                }

                final_selected_endpoints.append(endpoint_object)
                print(
                    f"Endpoint '{selected_ep_details['name']}' adicionado à configuração."
                )

            except ValueError:
                print("Entrada inválida. Por favor, digite um número ou 'q'.")

    @staticmethod
    def save_config_to_file(
        endpoints_details: List[Dict[str, Any]], config_path: str = DEFAULT_CONFIG_PATH
    ):
        """Salva os detalhes dos endpoints (objetos) em um arquivo de configuração JSON."""
        config_to_save = {"globus_compute_endpoints": endpoints_details}
        try:
            with open(config_path, "w") as f:
                json.dump(config_to_save, f, indent=4)
            print(f"\nConfiguração de endpoints salva em {config_path}")
        except IOError as e:
            print(f"Erro ao salvar o arquivo de configuração {config_path}: {e}")
            raise

    def configure_endpoints_interactive_and_save(
        self, save_to_path: Optional[str] = None
    ) -> bool:
        """
        Executa o processo interativo de seleção e configuração de endpoints,
        atualiza os endpoints ativos deste gerenciador e salva a configuração.
        """
        GlobusComputeCloudManager.login_interactive()

        selected_endpoints_config = (
            GlobusComputeCloudManager.select_endpoints_interactive()
        )

        if selected_endpoints_config:
            path_to_save = (
                save_to_path if save_to_path is not None else self._config_file_path
            )
            self.save_config_to_file(selected_endpoints_config, path_to_save)
            self._initialize_from_config(selected_endpoints_config)
            return True if self.active_endpoint_ids else False
        else:
            print("Nenhuma configuração de endpoint foi criada.")
            return False

    @staticmethod
    def list_endpoints() -> List[Dict[str, Any]]:
        """
        Busca e retorna uma lista de todos os endpoints disponíveis para
        o usuário autenticado, incluindo seus status.

        Acionará o fluxo de login interativo se o usuário não estiver autenticado.

        Returns:
            Uma lista de dicionários, onde cada dicionário contém detalhes
            de um endpoint ('uuid', 'name', 'status').
        """
        try:
            client = GlobusComputeClient()
            client.version_check()
        except Exception as e:
            print(f"\nERRO: Falha na autenticação com o Globus Compute: {e}")
            return []

        print("Buscando endpoints registrados...")
        available_endpoints: List[Dict[str, Any]] = []
        try:
            raw_endpoints = client.get_endpoints()
            if not raw_endpoints:
                return []

            for ep_info in raw_endpoints:
                try:
                    status_info = client.get_endpoint_status(ep_info["uuid"])
                    details = {
                        "uuid": ep_info["uuid"],
                        "name": ep_info.get("name", "Sem Nome"),
                        "status": status_info.get("status", "desconhecido"),
                    }
                    available_endpoints.append(details)
                except Exception:
                    details = {
                        "uuid": ep_info["uuid"],
                        "name": ep_info.get("name", "Sem Nome"),
                        "status": "ERRO ao obter status",
                    }
                    available_endpoints.append(details)
            return available_endpoints
        except Exception as e_list:
            print(f"ERRO: Não foi possível listar os endpoints: {e_list}")
            return []
