# mwfaas/master.py

from concurrent.futures import Future, as_completed
from typing import Callable, List, Union, Any, Optional, Dict
from .cloud_manager import CloudManager
from .distribution_strategy import DistributionStrategy
import cloudpickle
import traceback


class Master:
    """
    A classe Master orquestra a execução de tarefas paralelas em uma arquitetura FaaS
    utilizando o paradigma mestre-escravo.
    """

    def __init__(
        self,
        cloud_manager: CloudManager,
        distribution_strategy: DistributionStrategy,
    ):
        """
        Inicializa o Master.

        Args:
            cloud_manager: Uma instância de um gerenciador de nuvem.
            distribution_strategy: Uma instância opcional de uma estratégia de distribuição.
        """
        if cloud_manager is None:
            raise ValueError("Uma instância de cloud_manager é obrigatória.")
        if distribution_strategy is None:
            raise ValueError("Uma instância de distribution_strategy é obrigatória.")

        self.cloud_manager = cloud_manager
        self.distribution_strategy = distribution_strategy

        self._task_metadata: List[dict] = []

    def _wrap_user_function(
        self, user_function: Callable
    ) -> Callable[[List[Any]], Any]:
        """
        Cria e retorna uma nova função que 'embrulha' a função do usuário
        com uma lógica de tratamento de erro para o TypeError.
        Args:
            user_function: A função original fornecida pelo usuário.

        Returns:
            Uma nova função que, ao ser chamada, executa a original
            dentro de um bloco try/except.
        """

        def wrapped_function(chunk: List[Any]) -> Any:
            """Esta é a função que será de fato serializada e enviada ao worker."""
            try:
                return user_function(chunk)
            except TypeError as e:
                helpful_error_msg = (
                    "A função do usuário falhou com um TypeError. "
                    "Isso geralmente ocorre porque a função não foi projetada para receber uma lista ('chunk') de itens como argumento. "
                    "Lembre-se: a função do worker deve sempre aceitar uma lista e iterar sobre seus itens. "
                    f"Erro original: {e}"
                )
                raise ValueError(helpful_error_msg) from e

        return wrapped_function

    def _serialize_function(self, user_function: Callable[[Any], Any]) -> bytes:
        """Cria a função wrapper e a serializa usando cloudpickle."""
        wrapped_function = self._wrap_user_function(user_function)
        return cloudpickle.dumps(wrapped_function)

    def run(
        self, data_input: Any, user_function: Callable[[Any], Any]
    ) -> List[Union[Any, Exception]]:
        self._task_metadata = []

        if not callable(user_function):
            raise TypeError("user_function deve ser um objeto chamável (Callable).")

        serialized_function = self._serialize_function(user_function)

        available_workers = self.cloud_manager.get_active_worker_ids()
        num_workers = len(available_workers)

        if num_workers == 0:
            raise RuntimeError(
                "Nenhum worker ativo encontrado para executar as tarefas."
            )

        data_chunks = self.distribution_strategy.split_data(data_input, num_workers)
        if not data_chunks:
            return []

        chunk_iterator = iter(data_chunks)
        total_tasks = len(data_chunks)

        results: List[Optional[Union[Any, Exception]]] = [None] * total_tasks
        futures_to_index: Dict[Future, int] = {}
        next_task_index = 0
        print(
            f"Iniciando com um pool de {num_workers} worker(s) para {total_tasks} tarefa(s)..."
        )
        for i in range(min(num_workers, total_tasks)):
            chunk = next(chunk_iterator)
            worker_id = available_workers[i]

            future = self.cloud_manager.submit_task(
                worker_id, serialized_function, chunk
            )
            futures_to_index[future] = next_task_index
            next_task_index += 1

        # Processa e agenda dinamicamente
        processed_tasks = 0
        for future in as_completed(futures_to_index):
            original_index = futures_to_index[future]

            try:
                result = future.result()
                results[original_index] = result
            except Exception as e:
                traceback.print_exc()
                results[original_index] = e

            processed_tasks += 1
            print(f"Progresso: {processed_tasks}/{total_tasks} tarefas concluídas.")

            # Agenda a próxima tarefa no worker que ficou livre
            try:
                next_chunk = next(chunk_iterator)
                worker_id = available_workers[next_task_index % num_workers]

                print(f"Agendando chunk {next_task_index} no worker {worker_id}...")
                new_future = self.cloud_manager.submit_task(
                    worker_id, serialized_function, next_chunk
                )
                futures_to_index[new_future] = next_task_index
                next_task_index += 1

            except StopIteration:
                pass

        # A verificação final para 'None' ainda é uma boa prática
        final_results: List[Union[Any, Exception]] = []
        for item in results:
            if item is None:
                final_results.append(
                    RuntimeError(
                        "Resultado 'None' inesperado para um chunk processado."
                    )
                )
            else:
                final_results.append(item)

        return final_results

    def reduce(
        self,
        results_list: List[Union[Any, Exception]],
        reduce_function: Callable[[List[Any]], Any],
    ) -> Any:
        """
        Agrega uma lista de resultados parciais em um único resultado final.

        Este método implementa a fase "Reduce" do paradigma MapReduce. Ele primeiro
        filtra a lista de resultados para incluir apenas aqueles que não são exceções
        (ou seja, tarefas bem-sucedidas) e, em seguida, aplica a `reduce_function`
        fornecida pelo usuário a essa lista de resultados bem-sucedidos.

        Args:
            results_list: A lista de resultados retornada pelo método `run()`,
                          que pode conter resultados bem-sucedidos e objetos de Exceção.
            reduce_function: A função definida pelo usuário que sabe como combinar
                             os resultados. Ela deve aceitar um único argumento: uma lista
                             de resultados bem-sucedidos.

        Returns:
            O resultado final e agregado, do tipo retornado pela `reduce_function`.
            Retorna `None` se não houver resultados bem-sucedidos para agregar.
        """
        successful_results = [
            result for result in results_list if not isinstance(result, Exception)
        ]

        if not successful_results:
            print("Aviso: Nenhum resultado bem-sucedido para agregar. Retornando None.")
            return None

        try:
            print(
                f"Agregando {len(successful_results)} resultado(s) com a função '{reduce_function.__name__}'..."
            )
            final_result = reduce_function(successful_results)
            return final_result
        except Exception as e:
            print(f"ERRO: A função de agregação '{reduce_function.__name__}' falhou.")
            raise e

    def get_task_statuses(self) -> List[dict]:
        """Retorna uma cópia dos metadados sobre todas as tarefas da última execução de `run`."""
        return [status.copy() for status in self._task_metadata]

    # representation
    def __repr__(self) -> str:
        cm_name = (
            self.cloud_manager.__class__.__name__ if self.cloud_manager else "None"
        )
        ds_name = (
            self.distribution_strategy.__class__.__name__
            if self.distribution_strategy is not None
            else "None"
        )
        return f"<Master cloud_manager={cm_name}, distribution_strategy={ds_name}>"
