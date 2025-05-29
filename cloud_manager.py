# master_slave_faas/cloud_manager.py

import abc
from typing import Any
import uuid
import cloudpickle
from multiprocessing import Pool, TimeoutError as MPTimeoutError
import os

# Importa a classe Slave para ser usada pelo _execute_task_in_worker_process
# Isso assume que slave.py está na mesma estrutura de pacote e é acessível.
from .slave import Slave


# Esta função auxiliar será executada pelos processos de trabalho no Pool.
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

        # Cria uma instância do Slave e executa a tarefa
        # Cada tarefa FaaS (simulada) obtém sua própria instância de Slave.
        slave_instance = Slave()
        return slave_instance.execute_task(user_function, data_chunk)
    except Exception as e:
        # Retorna a própria instância da exceção.
        # O método .get() do AsyncResult no processo principal irá re-levantar esta exceção
        # se não for tratada aqui. Ao retorná-la, permitimos que o Master
        # diferencie entre um resultado bem-sucedido e um erro usando isinstance().
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
    Útil para desenvolvimento e testes sem a necessidade de implantação em nuvem real.
    """

    def __init__(self, num_workers: int | None = None):
        """
        Inicializa o LocalCloudManager.

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

        self._pool = None  # Inicializado "preguiçosamente" ou explicitamente
        self._active_tasks = {}  # Armazena task_id -> AsyncResult (do multiprocessing.Pool)
        self._ensure_pool_initialized()  # Garante que o pool seja criado na inicialização

    def _ensure_pool_initialized(self):
        if self._pool is None:
            # print(f"LocalCloudManager: Inicializando pool com {self._num_workers} workers.") # Para log
            self._pool = Pool(processes=self._num_workers)
            self._active_tasks = {}

    def get_target_parallelism(self) -> int:
        """Retorna o número de processos de trabalho no pool."""
        return self._num_workers

    def submit_task(self, serialized_function_bytes: bytes, data_chunk: Any) -> str:
        """
        Submete uma tarefa para o pool de multiprocessing local.
        """
        self._ensure_pool_initialized()  # Garante que o pool exista
        if (
            self._pool is None
        ):  # Verificação adicional caso _ensure_pool_initialized falhe ou seja contornada
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
        except Exception as e:  # Ex: se o pool estiver fechando
            # from .exceptions import TaskSubmissionError # Usar exceções customizadas
            # raise TaskSubmissionError(f"Falha ao submeter tarefa {task_id} localmente: {e}") from e
            # Por enquanto, levanta uma exceção genérica ou específica do runtime
            print(
                f"ERRO ao submeter tarefa {task_id} para o pool local: {e}"
            )  # Substituir por logging
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
        if (
            self._pool is None and self._active_tasks
        ):  # Pool desligado mas ainda há tarefas ativas (improvável)
            raise RuntimeError(
                "LocalCloudManager foi desligado, mas ainda existem tarefas ativas pendentes."
            )
        if (
            not self._active_tasks and task_ids
        ):  # Sem tarefas ativas mas IDs foram solicitados
            # Isso pode acontecer se submit_task falhou para todos os IDs
            # ou se os IDs são inválidos.
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
                # from .exceptions import ResultCollectionError # Usar exceções customizadas
                # outcomes.append(ResultCollectionError(f"ID de tarefa desconhecido: {task_id}"))
                outcomes.append(
                    KeyError(f"ID de tarefa desconhecido ou já processado: {task_id}")
                )
                continue

            try:
                # .get() irá re-levantar exceções que ocorreram no processo de trabalho
                # (a menos que o worker as capture e retorne explicitamente, como o nosso faz).
                outcome = async_result.get(timeout=timeout_per_task)
                outcomes.append(outcome)
            except MPTimeoutError:  # from multiprocessing.TimeoutError
                # from .exceptions import TaskTimeoutError # Usar exceções customizadas
                # outcomes.append(TaskTimeoutError(f"Tarefa {task_id} excedeu o tempo limite de {timeout_per_task}s"))
                outcomes.append(
                    MPTimeoutError(
                        f"Tarefa {task_id} excedeu o tempo limite ({timeout_per_task}s)."
                    )
                )
            except Exception as e:
                # Erro inesperado ao tentar obter o resultado (não um erro da tarefa em si).
                print(
                    f"ERRO inesperado ao obter resultado para tarefa {task_id}: {e}"
                )  # Substituir por logging
                outcomes.append(e)

            # Opcional: remover a tarefa de _active_tasks após obter o resultado para liberar memória,
            # mas isso impede a consulta de status posterior para o mesmo task_id.
            # Se a política for "obter resultado uma vez", então del self._active_tasks[task_id] é ok.
            # Por enquanto, mantemos para o caso de futuras consultas de status.

        return outcomes

    def shutdown(self):
        """Fecha o pool de multiprocessing e aguarda a conclusão dos workers."""
        if self._pool is not None:
            # print("LocalCloudManager: Desligando o pool de processos...") # Substituir por logging
            try:
                self._pool.close()  # Impede que novas tarefas sejam submetidas.
                self._pool.join()  # Aguarda a saída dos processos de trabalho.
            except Exception as e:
                print(
                    f"LocalCloudManager: Erro ao desligar o pool: {e}"
                )  # Substituir por logging
            finally:
                self._pool = None
                self._active_tasks = {}  # Limpa as tarefas ativas
                # print("LocalCloudManager: Pool de processos desligado.")
