# mwfaas/cloud_manager.py

import abc
from typing import Any
import uuid
import cloudpickle
from multiprocessing import Pool, TimeoutError as MPTimeoutError
import os


# Deve ser definida no nível superior de um módulo para ser serializável (picklable) pelo multiprocessing.
def _execute_task_in_worker_process(
    serialized_user_function_bytes: bytes, data_chunk: Any
) -> Any:
    """
    Função de trabalho que executa em um processo separado (simulando um endpoint FaaS).
    Ela desserializa a função do usuário e a executa sobre o data_chunk usando uma instância de Slave.

    Args:
        serialized_user_function_bytes: A função do usuário, serializada como bytes.
        data_chunk: O pedaço de dados para esta tarefa.

    Returns:
        O resultado da execução da tarefa ou uma instância de Exception se ocorrer um erro.
    """
    try:
        # Desserializa a função do usuário
        user_function = cloudpickle.loads(serialized_user_function_bytes)
        return user_function(data_chunk)
    except Exception as e:
        return e


class BaseCloudManager(abc.ABC):
    """
    Classe base abstrata para Gerenciadores de Nuvem (Cloud Managers).
    Define a interface para interagir com um backend FaaS (Function as a Service).
    """

    @abc.abstractmethod
    def get_target_parallelism(self) -> int:
        """
        Retorna o número recomendado ou disponível de workers/endpoints paralelos.
        Este valor influencia como a carga de trabalho é dividida.
        """
        pass

    @abc.abstractmethod
    def submit_task(self, serialized_function_bytes: bytes, data_chunk: Any) -> str:
        """
        Submete uma tarefa (uma função serializada e um bloco de dados) para execução.

        Args:
            serialized_function_bytes: A função do usuário, serializada (por exemplo, com cloudpickle).
            data_chunk: O bloco de dados para esta tarefa.

        Returns:
            Um ID de tarefa único (string).
        """
        pass

    @abc.abstractmethod
    def get_all_results_for_ids(
        self, task_ids: list, timeout_per_task: float | None = None
    ) -> list:
        """
        Recupera os resultados para uma lista de IDs de tarefas.
        Este método deve bloquear até que todas as tarefas especificadas sejam concluídas
        ou tenham falhado (ou um timeout ocorra).

        Args:
            task_ids: Uma lista de IDs de tarefas para as quais buscar resultados.
            timeout_per_task (float, optional): Timeout em segundos para a obtenção do resultado
                                                de cada tarefa individual. Se None, aguarda indefinidamente.

        Returns:
            Uma lista de resultados/exceções. Cada item na lista é ou o resultado
            bem-sucedido da tarefa ou um objeto Exception se a tarefa falhou.
            A ordem dos resultados na lista DEVE corresponder à ordem dos task_IDs na lista de entrada.
        """
        pass

    def shutdown(self):
        """
        Opcional: Realiza qualquer limpeza necessária, como desligar um pool de processos.
        A implementação padrão não faz nada. As subclasses devem sobrescrever se necessário.
        """
        # print(f"Shutdown chamado em {self.__class__.__name__}, nenhuma ação padrão.")
        pass

    def __enter__(self):
        """Permite o uso como gerenciador de contexto (context manager)."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Garante que shutdown() seja chamado ao sair do contexto."""
        self.shutdown()


class LocalCloudManager(BaseCloudManager):
    """
    Um CloudManager que simula a execução FaaS localmente usando um `multiprocessing.Pool`.
    """

    def __init__(self, num_workers: int | None = None):
        """
        Args:
            num_workers (int, optional): O número de processos de trabalho a serem usados no pool.
                                         Padrão para `os.cpu_count()`.
        """
        if num_workers is None:
            self._num_workers = os.cpu_count() or 1  # Garante pelo menos 1 worker
        elif not isinstance(num_workers, int) or num_workers <= 0:
            raise ValueError("num_workers deve ser um inteiro positivo.")
        else:
            self._num_workers = num_workers

        self._pool = None
        self._active_tasks = {}
        self._ensure_pool_initialized()

    def _ensure_pool_initialized(self):
        if self._pool is None:
            self._pool = Pool(processes=self._num_workers)
            self._active_tasks = {}

    def get_target_parallelism(self) -> int:
        """Retorna o número de processos de trabalho no pool."""
        return self._num_workers

    def submit_task(self, serialized_function_bytes: bytes, data_chunk: Any) -> str:
        """
        Submete uma tarefa para o pool de multiprocessing local.
        """
        self._ensure_pool_initialized()
        if self._pool is None:
            raise RuntimeError(
                "LocalCloudManager não foi inicializado corretamente ou já foi desligado."
            )

        task_id = str(uuid.uuid4())
        try:
            async_result = self._pool.apply_async(
                _execute_task_in_worker_process,  # Função de trabalho global
                (serialized_function_bytes, data_chunk),
            )
            self._active_tasks[task_id] = async_result
            return task_id
        except Exception as e:
            print(f"ERRO ao submeter tarefa {task_id} para o pool local: {e}")
            raise RuntimeError(
                f"Falha ao submeter tarefa {task_id} localmente: {e}"
            ) from e

    def get_all_results_for_ids(
        self, task_ids: list, timeout_per_task: float | None = None
    ) -> list:
        """
        Recupera os resultados para os task_ids fornecidos do pool local.
        Bloqueia até que todas as tarefas sejam concluídas ou um timeout ocorra.
        Os resultados são retornados na mesma ordem dos task_ids.
        """
        if self._pool is None and self._active_tasks:
            raise RuntimeError(
                "LocalCloudManager foi desligado, mas ainda existem tarefas ativas pendentes."
            )
        if not self._active_tasks and task_ids:
            return [
                KeyError(
                    f"Nenhuma tarefa ativa encontrada, ou ID de tarefa desconhecido: {tid}"
                )
                for tid in task_ids
            ]

        outcomes = []
        for task_id in task_ids:
            async_result = self._active_tasks.get(task_id)

            if async_result is None:
                outcomes.append(
                    KeyError(f"ID de tarefa desconhecido ou já processado: {task_id}")
                )
                continue

            try:
                outcome = async_result.get(timeout=timeout_per_task)
                outcomes.append(outcome)
            except MPTimeoutError:
                outcomes.append(
                    MPTimeoutError(
                        f"Tarefa {task_id} excedeu o tempo limite ({timeout_per_task}s)."
                    )
                )
            except Exception as e:
                print(f"ERRO inesperado ao obter resultado para tarefa {task_id}: {e}")
                outcomes.append(e)

        return outcomes

    def shutdown(self):
        """Fecha o pool de multiprocessing e aguarda a conclusão dos workers."""
        if self._pool is not None:
            print("LocalCloudManager: Desligando o pool de processos...")
            try:
                self._pool.close()  # Impede que novas tarefas sejam submetidas.
                self._pool.join()  # Aguarda a saída dos processos de trabalho.
            except Exception as e:
                print(f"LocalCloudManager: Erro ao desligar o pool: {e}")
            finally:
                self._pool = None
                self._active_tasks = {}
                print("LocalCloudManager: Pool de processos desligado.")
