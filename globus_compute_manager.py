# master_slave_faas/globus_compute_manager.py

import json
import re
import uuid
import cloudpickle
from typing import Any, Optional, List, Dict

from globus_compute_sdk import Client as GlobusComputeClient
from globus_compute_sdk import Executor
from globus_compute_sdk.sdk.executor import ComputeFuture  # Para type hint
from concurrent.futures import (
    TimeoutError as FuturesTimeoutError,
)

from .cloud_manager import BaseCloudManager
# from ..exceptions import TaskSubmissionError, ResultCollectionError, TaskTimeoutError # Exceções customizadas

# Constante para o arquivo de configuração padrão
DEFAULT_CONFIG_PATH = "master_slave_globus_config.json"


class GlobusComputeCloudManager(BaseCloudManager):
    """
    Um CloudManager para interagir com o Globus Compute.
    Ele lida com autenticação, seleção de endpoints, submissão de tarefas e coleta de resultados.
    """

    def __init__(
        self,
        endpoint_ids: Optional[List[str]] = None,
        config_file_path: str = DEFAULT_CONFIG_PATH,
        auto_authenticate: bool = True,
    ):
        """
        Inicializa o GlobusComputeCloudManager.

        Args:
            endpoint_ids: Uma lista opcional de IDs de endpoint do Globus Compute a serem usados.
                          Se não fornecido, tentará carregar de `config_file_path`.
            config_file_path: Caminho para o arquivo de configuração para carregar/salvar IDs de endpoint.
            auto_authenticate: Se True, tenta garantir a autenticação na inicialização.
        """
        self._config_file_path = config_file_path
        self._client = GlobusComputeClient()

        self.active_endpoint_ids: List[str] = []
        self._executors: Dict[str, Executor] = {}  # endpoint_id -> Executor
        self._active_tasks: Dict[
            str, ComputeFuture
        ] = {}  # internal_task_id -> GlobusComputeFuture
        self._next_endpoint_idx = 0  # Para round-robin na submissão de tarefas

        if auto_authenticate:
            # Tenta uma operação leve para acionar o fluxo de login se necessário.
            # A autenticação real é lazy e gerenciada pelo SDK.
            try:
                self._client.version_check()
                # print("Globus Compute SDK: Autenticação parece estar ativa.") # Log
            except Exception as e:
                print(
                    f"Aviso: A verificação inicial de autenticação com Globus Compute falhou: {e}. "
                    "A autenticação será tentada novamente na primeira operação."
                )

        loaded_ids = []
        if endpoint_ids:
            loaded_ids = endpoint_ids
            # print(f"Usando endpoints fornecidos diretamente: {loaded_ids}") # Log
        else:
            loaded_ids = self._load_config_from_file(self._config_file_path)
            if loaded_ids:
                # print(f"Endpoints carregados de {self._config_file_path}: {loaded_ids}") # Log
                pass
            else:
                print(
                    f"Nenhum endpoint ID fornecido e nenhum encontrado em '{self._config_file_path}'."
                )
                print(
                    "Considere executar o método 'configure_endpoints_interactive()' "
                    "ou fornecer 'endpoint_ids' na inicialização."
                )

        if loaded_ids:
            self._initialize_executors(loaded_ids)

    def _initialize_executors(self, endpoint_ids_to_init: List[str]):
        """Inicializa os executores para os IDs de endpoint fornecidos."""
        # Limpa executores e endpoints ativos existentes
        self.shutdown_executors()  # Garante que executores antigos sejam desligados

        new_active_ids = []
        new_executors = {}

        for ep_id in endpoint_ids_to_init:
            try:
                # Verificar o status do endpoint antes de criar o executor pode ser útil,
                # mas o Executor também pode falhar se o endpoint não estiver acessível.
                # status_info = self._client.get_endpoint_status(ep_id)
                # if status_info.get('status') != 'online':
                #     print(f"Aviso: Endpoint Globus Compute {ep_id} não está 'online' (status: {status_info.get('status')}). Não será utilizado.")
                #     continue

                # Passar o self._client garante que usem a mesma sessão autenticada
                new_executors[ep_id] = Executor(endpoint_id=ep_id, client=self._client)
                new_active_ids.append(ep_id)
                # print(f"Executor Globus Compute inicializado para endpoint: {ep_id}") # Log
            except Exception as e:
                print(
                    f"Aviso: Falha ao criar GlobusComputeExecutor para o endpoint {ep_id}: {e}. Este endpoint não será utilizado."
                )

        self.active_endpoint_ids = new_active_ids
        self._executors = new_executors

        if not self.active_endpoint_ids:
            print(
                "Aviso: Nenhum executor Globus Compute utilizável pôde ser inicializado."
            )

    def _load_config_from_file(self, config_path: str) -> List[str]:
        """Carrega IDs de endpoint de um arquivo de configuração JSON."""
        try:
            with open(config_path, "r") as f:
                config = json.load(f)
                ids = config.get("globus_compute_endpoint_ids", [])
                if isinstance(ids, list) and all(isinstance(item, str) for item in ids):
                    return ids
                else:
                    print(
                        f"Erro: Formato inválido para 'globus_compute_endpoint_ids' em {config_path}."
                    )
                    return []
        except FileNotFoundError:
            # print(f"Info: Arquivo de configuração {config_path} não encontrado.") # Log
            return []
        except json.JSONDecodeError:
            print(f"Erro: Não foi possível decodificar JSON de {config_path}.")
            return []
        except Exception as e:
            print(f"Erro inesperado ao carregar configuração de {config_path}: {e}")
            return []

    # --- Implementação dos Métodos Abstratos de BaseCloudManager ---

    def get_target_parallelism(self) -> int:
        """
        Retorna o número de endpoints Globus Compute ativos e utilizáveis.
        Pode ser ajustado para refletir o total de workers, se essa informação estiver disponível.
        """
        if not self.active_endpoint_ids or not self._executors:
            return 0
        # Cada endpoint é um "alvo" para paralelismo ao nível do Master.
        # O número real de workers *dentro* de cada endpoint é gerenciado pelo Globus Compute.
        return len(self.active_endpoint_ids)

    def submit_task(self, serialized_function_bytes: bytes, data_chunk: Any) -> str:
        """
        Submete uma tarefa a um dos endpoints Globus Compute configurados (usando round-robin).
        A função é desserializada antes da submissão, pois GlobusComputeExecutor espera um callable.
        """
        if not self._executors or not self.active_endpoint_ids:
            # from ..exceptions import TaskSubmissionError
            raise RuntimeError(
                "Nenhum executor Globus Compute está disponível/configurado para submissão de tarefas."
            )  # TaskSubmissionError

        try:
            user_function = cloudpickle.loads(serialized_function_bytes)
        except Exception as e:
            # from ..exceptions import FunctionSerializationError (ou um erro de desserialização)
            raise ValueError(
                f"Falha ao desserializar a função do usuário para o Globus Compute: {e}"
            ) from e

        # Seleção Round-robin do endpoint
        selected_endpoint_id = self.active_endpoint_ids[
            self._next_endpoint_idx % len(self.active_endpoint_ids)
        ]
        self._next_endpoint_idx += 1

        executor = self._executors[selected_endpoint_id]
        internal_task_id = str(uuid.uuid4())  # ID interno para rastrear o Future

        try:
            # GlobusComputeExecutor.submit(func, *args, **kwargs)
            future: ComputeFuture = executor.submit(user_function, data_chunk)
            self._active_tasks[internal_task_id] = future
            # print(f"Tarefa {internal_task_id} (GC UUID: {future.task_uuid}) submetida ao endpoint {selected_endpoint_id}") # Log
            return internal_task_id
        except Exception as e:
            # from ..exceptions import TaskSubmissionError
            print(
                f"ERRO ao submeter tarefa ao endpoint Globus Compute {selected_endpoint_id}: {e}"
            )  # Log
            raise RuntimeError(
                f"Falha ao submeter tarefa ao endpoint Globus Compute {selected_endpoint_id}: {e}"
            ) from e  # TaskSubmissionError

    def get_all_results_for_ids(
        self, task_ids: List[str], timeout_per_task: Optional[float] = None
    ) -> List[Any]:
        """
        Recupera os resultados para uma lista de IDs de tarefas internas.
        Bloqueia até que todas as tarefas sejam concluídas, falhem ou atinjam o timeout.
        """
        outcomes: List[Any] = []
        for internal_task_id in task_ids:
            future = self._active_tasks.get(internal_task_id)

            if future is None:
                # from ..exceptions import ResultCollectionError
                outcomes.append(
                    KeyError(
                        f"ID de tarefa interno desconhecido: {internal_task_id} para Globus Compute."
                    )
                )
                continue

            try:
                # ComputeFuture.result() levanta a exceção da tarefa se ela falhou.
                result = future.result(timeout=timeout_per_task)
                outcomes.append(result)
            except FuturesTimeoutError:  # concurrent.futures.TimeoutError
                # from ..exceptions import TaskTimeoutError
                gc_uuid = future.task_id if hasattr(future, "task_uuid") else "N/A"
                outcomes.append(
                    FuturesTimeoutError(
                        f"Tarefa Globus Compute (ID interno: {internal_task_id}, GC ID: {gc_uuid}) "
                        f"excedeu o tempo limite de {timeout_per_task}s."
                    )  # TaskTimeoutError
                )
            except Exception as e:
                # Esta 'e' é a exceção levantada pela execução da função do usuário no endpoint.
                outcomes.append(e)
            # Opcional: remover de self._active_tasks após obter o resultado para economizar memória
            # del self._active_tasks[internal_task_id]
        return outcomes

    def shutdown_executors(self):
        """Desliga todos os executores Globus Compute ativos."""
        if not self._executors:
            return
        # print("Desligando executores Globus Compute...") # Log
        for endpoint_id, executor in self._executors.items():
            try:
                # print(f"Desligando executor para o endpoint {endpoint_id}...") # Log
                executor.shutdown(wait=True)  # Espera tarefas em andamento (padrão)
            except Exception as e:
                print(
                    f"Erro ao desligar o executor para o endpoint {endpoint_id}: {e}"
                )  # Log
        self._executors.clear()
        self._active_tasks.clear()  # Tarefas ativas tornam-se inválidas se os executores são desligados
        # print("Executores Globus Compute desligados.") # Log

    def shutdown(self):
        """Método de limpeza para o BaseCloudManager."""
        self.shutdown_executors()

    # --- Métodos de Configuração e Autenticação (Adaptados do Script) ---

    @staticmethod
    def do_logout():  # Renomeado para evitar conflito com possíveis frameworks
        """Realiza a desautenticação (logout) com o Globus Compute."""
        print("\n--- Desautenticação Globus ---")
        try:
            client = GlobusComputeClient()
            client.logout()
            print("Desautenticação Globus bem-sucedida.")
        except Exception as e:
            print(f"Erro durante a desautenticação: {e}")

    @staticmethod
    def do_login_interactive() -> bool:  # Renomeado
        """Realiza a autenticação interativa com o Globus Compute."""
        print("\n--- Autenticação Globus ---")
        print(
            "Siga as instruções na tela/navegador para completar o processo de login."
        )
        try:
            client = GlobusComputeClient()
            client.version_check()  # Operação leve para forçar o fluxo de login se necessário
            print("Autenticação Globus parece estar ativa ou foi bem-sucedida.")
            return True
        except Exception as e:
            print(f"Erro durante a tentativa de autenticação/verificação: {e}")
            print(
                "Por favor, tente novamente ou verifique sua configuração/ambiente Globus."
            )
            return False

    @staticmethod
    def select_endpoints_interactive() -> List[str]:  # Renomeado
        """
        Permite ao usuário selecionar interativamente um ou mais endpoints Globus Compute existentes.
        Retorna uma lista de IDs de endpoints selecionados que estão 'online'.
        """
        print("\n--- Seleção Interativa de Endpoints Globus Compute ---")

        # Garante autenticação antes de listar endpoints
        # GlobusComputeCloudManager.do_login_interactive() # Pode ser chamado antes externamente se necessário

        client = GlobusComputeClient()
        print("Listando seus endpoints Globus Compute existentes...")

        available_endpoints_details = []
        try:
            raw_endpoints = client.get_endpoints()
            if not raw_endpoints:
                print("Nenhum endpoint encontrado na sua conta Globus Compute.")
            for i, ep_info in enumerate(raw_endpoints):
                try:
                    status_info = client.get_endpoint_status(ep_info["uuid"])
                    details = {
                        "uuid": ep_info["uuid"],
                        "name": ep_info["name"],
                        "status": status_info["status"],
                    }
                    available_endpoints_details.append(details)
                    print(
                        f"  {i + 1} - Nome: {details['name']} (ID: {details['uuid']}) - Status: {details['status']}"
                    )
                except Exception as e_status:
                    print(
                        f"  Erro ao obter status para endpoint {ep_info.get('name', ep_info.get('uuid', 'Desconhecido'))}: {e_status}"
                    )
        except Exception as e_list:
            print(f"Não foi possível listar os endpoints: {e_list}")
            print(
                "Certifique-se de que está autenticado e o serviço Globus Compute está disponível."
            )
            return []

        if not available_endpoints_details:
            print("\nNenhum endpoint do Globus Compute encontrado ou acessível.")
            # (Instruções para configurar um novo endpoint, como no script original)
            print(
                "Para configurar um novo endpoint, siga as instruções da documentação do Globus Compute:"
            )
            print("  1. Instale: pip install globus-compute-endpoint")
            print(
                "  2. Configure: globus-compute-endpoint configure <NOME_DO_ENDPOINT>"
            )
            print("  3. Inicie: globus-compute-endpoint start <NOME_DO_ENDPOINT>")
            return []

        selected_endpoint_ids = []
        while True:
            user_input = (
                input(
                    "\nDigite o(s) número(s) dos endpoints que deseja usar (separados por espaço/vírgula, 'todos' para selecionar todos os online, ou 'q' para sair): "
                )
                .strip()
                .lower()
            )

            if user_input == "q":
                return []

            if user_input == "todos":
                ids_to_add = [
                    ep["uuid"]
                    for ep in available_endpoints_details
                    if ep["status"] == "online"
                ]
                if not ids_to_add:
                    print(
                        "Nenhum endpoint 'online' encontrado para selecionar com 'todos'."
                    )
                    continue
                selected_endpoint_ids.extend(ids_to_add)
                # Remover duplicatas se o usuário digitar 'todos' múltiplas vezes ou misturar com números
                selected_endpoint_ids = sorted(list(set(selected_endpoint_ids)))
                print(
                    f"Endpoints 'online' selecionados: {', '.join(selected_endpoint_ids)}"
                )
                return selected_endpoint_ids

            # Usa re.findall para encontrar todos os grupos de dígitos
            selected_numbers_str = re.findall(r"\d+", user_input)
            if not selected_numbers_str:
                print("Nenhum número detectado na entrada. Tente novamente.")
                continue

            try:
                selected_numbers = [int(num_str) for num_str in selected_numbers_str]
            except ValueError:
                print(
                    "Entrada inválida. Por favor, digite apenas números, 'todos' ou 'q'."
                )
                continue

            current_selection_round_ids = []
            valid_selection_this_round = True
            for num in selected_numbers:
                index = num - 1  # Ajusta para índice base 0
                if 0 <= index < len(available_endpoints_details):
                    ep_data = available_endpoints_details[index]
                    if ep_data["status"] != "online":
                        print(
                            f"AVISO: Endpoint '{ep_data['name']}' (ID: {ep_data['uuid']}) não está 'online' (status: {ep_data['status']}). "
                            "Ele não será adicionado. Por favor, inicie-o se desejar usá-lo."
                        )
                        # Não torna a seleção inteira inválida, apenas pula este endpoint
                    else:
                        current_selection_round_ids.append(ep_data["uuid"])
                else:
                    print(
                        f"Número de endpoint inválido: {num}. Por favor, verifique e tente novamente."
                    )
                    valid_selection_this_round = (
                        False  # Invalida esta rodada de números
                    )
                    current_selection_round_ids = []  # Limpa o que foi selecionado nesta rodada
                    break

            if valid_selection_this_round and current_selection_round_ids:
                selected_endpoint_ids.extend(current_selection_round_ids)
                # Remover duplicatas e ordenar
                selected_endpoint_ids = sorted(list(set(selected_endpoint_ids)))
                print(
                    f"Endpoint(s) atualmente selecionado(s): {', '.join(selected_endpoint_ids) if selected_endpoint_ids else 'Nenhum'}"
                )

                confirm = (
                    input(
                        "Deseja adicionar mais endpoints (s/N) ou finalizar a seleção (ENTER para finalizar)? "
                    )
                    .strip()
                    .lower()
                )
                if confirm != "s":
                    if selected_endpoint_ids:
                        print(
                            f"Seleção finalizada. Endpoints escolhidos: {', '.join(selected_endpoint_ids)}"
                        )
                        return selected_endpoint_ids
                    else:
                        print("Nenhum endpoint válido foi selecionado.")
                        # Loop continua para nova tentativa ou 'q'
            elif (
                not current_selection_round_ids and valid_selection_this_round
            ):  # Nenhum número válido resultou em um endpoint
                print("Nenhum endpoint válido adicionado nesta rodada.")

    @staticmethod
    def save_config_to_file(
        endpoint_ids: List[str], config_path: str = DEFAULT_CONFIG_PATH
    ):
        """Salva os IDs dos endpoints do Globus Compute em um arquivo de configuração JSON."""
        config_to_save = {"globus_compute_endpoint_ids": endpoint_ids}
        try:
            with open(config_path, "w") as f:
                json.dump(config_to_save, f, indent=4)
            print(f"\nConfiguração de endpoints salva em {config_path}")
        except IOError as e:
            print(f"Erro ao salvar o arquivo de configuração {config_path}: {e}")
            # Em uma classe de biblioteca, é melhor levantar uma exceção do que sair
            raise  # Ou raise CustomConfigSaveError(f"...") from e

    def configure_endpoints_interactive_and_save(
        self, save_to_path: Optional[str] = None, auto_activate_session: bool = True
    ) -> bool:
        """
        Executa o processo interativo de seleção de endpoints, atualiza os endpoints
        ativos deste gerenciador e, opcionalmente, salva a configuração.

        Args:
            save_to_path: Caminho para salvar o arquivo de configuração. Se None, usa
                          o path de configuração padrão do gerenciador.
            auto_activate_session: Se True, tenta autenticar antes de listar endpoints.

        Returns:
            True se a configuração foi bem-sucedida e pelo menos um endpoint foi ativado,
            False caso contrário.
        """
        if auto_activate_session:
            print("Verificando/ativando sessão Globus Compute...")
            if not GlobusComputeCloudManager.do_login_interactive():
                print(
                    "Falha na autenticação. Não é possível prosseguir com a configuração do endpoint."
                )
                return False

        selected_ids = GlobusComputeCloudManager.select_endpoints_interactive()

        if selected_ids:
            self._initialize_executors(
                selected_ids
            )  # Reconfigura os executores com os novos IDs

            if not self.active_endpoint_ids:
                print(
                    "Configuração falhou: Nenhum dos endpoints selecionados pôde ser ativado como executor."
                )
                return False

            path_to_save = (
                save_to_path if save_to_path is not None else self._config_file_path
            )
            GlobusComputeCloudManager.save_config_to_file(
                self.active_endpoint_ids, path_to_save
            )
            print(
                f"Gerenciador Globus Compute configurado com endpoint(s): {', '.join(self.active_endpoint_ids)}"
            )
            return True
        else:
            print(
                "Nenhum endpoint foi selecionado ou a seleção foi cancelada. "
                "A configuração do gerenciador permanece inalterada ou vazia."
            )
            # Mantém os endpoints atuais se a nova seleção falhar em obter algum ID
            if (
                not self.active_endpoint_ids
            ):  # Se não havia nenhum antes e nenhum foi selecionado
                return False
            return True  # Retorna True se já tinha endpoints ativos e o usuário cancelou a nova seleção
