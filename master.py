# mwfaas/master.py

from typing import Callable, List, Union, Any, Optional
from .cloud_manager import CloudManager
from .distribution import DistributionStrategy
import cloudpickle


class Master:
    """
    A classe Master orquestra a execução de tarefas paralelas em uma arquitetura FaaS
    utilizando o paradigma mestre-escravo.
    """

    def __init__(
        self,
        cloud_manager: CloudManager,
        distribution_strategy: Optional[DistributionStrategy] = None,
    ):
        """
        Inicializa o Master.

        Args:
            cloud_manager: Uma instância de um gerenciador de nuvem.
            distribution_strategy: Uma instância opcional de uma estratégia de distribuição.
        """
        if cloud_manager is None:
            raise ValueError("Uma instância de cloud_manager é obrigatória.")
        self.cloud_manager = cloud_manager

        if distribution_strategy is None:
            from .distribution import DefaultDistributionStrategy

            self.distribution_strategy = DefaultDistributionStrategy()
        else:
            if not hasattr(distribution_strategy, "split_data"):
                raise TypeError(
                    "A instância de distribution_strategy deve possuir um método 'split_data'."
                )
            self.distribution_strategy = distribution_strategy

        self._user_function_serialized: Optional[bytes] = None
        self._task_metadata: List[dict] = []

    def _serialize_function(self, user_function: Callable[[Any], Any]):
        """Serializa a função usando cloudpickle."""
        try:
            self._user_function_serialized = cloudpickle.dumps(user_function)
        except Exception as e:
            raise e

    def run(
        self, data_input: List[Any], user_function: Callable[[Any], Any]
    ) -> List[Union[Any, Exception]]:
        """
        Orquestra o processamento paralelo de `data_input` usando `user_function`.

        Args:
            data_input: A lista de dados a ser processada (cada item é 'Any').
            user_function: A função Python que aceita um item de 'data_input' ('Any')
                           e retorna um resultado ('Any').

        Returns:
            list: Uma lista contendo os resultados (do tipo 'Any') de cada bloco de dados
                  ou um objeto de Exceção se a tarefa correspondente falhou. A ordem é preservada.
        """
        self._task_metadata = []

        if not callable(user_function):
            raise TypeError("user_function deve ser um objeto chamável (Callable).")

        self._serialize_function(user_function)

        try:
            num_target_splits = self.cloud_manager.get_worker_count()
            if not (isinstance(num_target_splits, int) and num_target_splits > 0):
                raise ValueError(
                    f"O método get_worker_count() do CloudManager deve retornar um "
                    f"inteiro positivo, mas retornou: {num_target_splits}"
                )
        except AttributeError:
            raise AttributeError(
                "A instância de CloudManager deve implementar o método get_worker_count()."
            ) from None
        except Exception as e_parallelism:
            raise RuntimeError(
                f"Erro ao chamar get_worker_count() no CloudManager: {e_parallelism}"
            ) from e_parallelism

        try:
            data_chunks = self.distribution_strategy.split_data(
                data_input, num_target_splits
            )
        except ValueError as e_split_value:
            raise ValueError(
                f"Argumentos inválidos para a divisão de dados: {e_split_value}"
            ) from e_split_value
        except Exception as e_split:
            raise RuntimeError(
                f"Falha ao dividir os dados usando {self.distribution_strategy.__class__.__name__}: {e_split}"
            ) from e_split

        if not data_chunks and not data_input:
            return []

        # list of any, exception or None
        collected_results_ordered: List[Optional[Union[Any, Exception]]]
        collected_results_ordered = [None] * len(data_chunks)

        tasks_for_result_collection: List[dict] = []

        for i, chunk in enumerate(data_chunks):
            try:
                if self._user_function_serialized is None:
                    raise RuntimeError(
                        "Função do usuário não foi serializada corretamente."
                    )
                task_id = self.cloud_manager.submit_task(
                    self._user_function_serialized, chunk
                )
                tasks_for_result_collection.append({"original_index": i, "id": task_id})
                self._task_metadata.append(
                    {"id": task_id, "chunk_index": i, "status": "submitted"}
                )
            except Exception as e_submit:
                self._task_metadata.append(
                    {
                        "id": None,
                        "chunk_index": i,
                        "status": "submission_failed",
                        "error": e_submit,
                    }
                )
                collected_results_ordered[i] = e_submit

        task_ids_to_fetch = [
            task["id"] for task in tasks_for_result_collection if task["id"] is not None
        ]
        print(f"IDs das tarefas a serem coletadas: {task_ids_to_fetch}")

        if task_ids_to_fetch:
            try:
                task_outcomes = self.cloud_manager.get_results_for_ids(
                    task_ids_to_fetch
                )

                if len(task_outcomes) != len(
                    task_ids_to_fetch
                ):  # handle inconsistent results
                    err_msg = RuntimeError(
                        "Contagem de resultados do CloudManager inconsistente."
                    )
                    processed_ids_indices = {
                        task_info["id"]: task_info["original_index"]
                        for task_info in tasks_for_result_collection
                    }
                    for k, outcome_idx in enumerate(range(len(task_outcomes))):
                        task_id_processed = task_ids_to_fetch[k]
                        original_chunk_idx = processed_ids_indices.pop(
                            task_id_processed
                        )
                        meta_record = next(
                            (
                                m
                                for m in self._task_metadata
                                if m["id"] == task_id_processed
                            ),
                            None,
                        )
                        if isinstance(task_outcomes[outcome_idx], Exception):
                            if meta_record:
                                meta_record["status"], meta_record["error"] = (
                                    "failed",
                                    task_outcomes[outcome_idx],
                                )
                        else:
                            if meta_record:
                                meta_record["status"], meta_record["result"] = (
                                    "completed",
                                    task_outcomes[outcome_idx],
                                )
                        collected_results_ordered[original_chunk_idx] = task_outcomes[
                            outcome_idx
                        ]

                    for (
                        remaining_task_id,
                        original_chunk_idx_remaining,
                    ) in processed_ids_indices.items():
                        collected_results_ordered[original_chunk_idx_remaining] = (
                            err_msg
                        )
                        meta_record = next(
                            (
                                m
                                for m in self._task_metadata
                                if m["id"] == remaining_task_id
                            ),
                            None,
                        )
                        if meta_record:
                            meta_record["status"], meta_record["error"] = (
                                "failed",
                                err_msg,
                            )
                else:
                    for idx, outcome in enumerate(task_outcomes):
                        original_chunk_index = tasks_for_result_collection[idx][
                            "original_index"
                        ]
                        task_id_for_outcome = tasks_for_result_collection[idx]["id"]
                        meta_record = next(
                            (
                                m
                                for m in self._task_metadata
                                if m["id"] == task_id_for_outcome
                            ),
                            None,
                        )
                        if isinstance(outcome, Exception):
                            if meta_record:
                                meta_record["status"], meta_record["error"] = (
                                    "failed",
                                    outcome,
                                )
                        else:
                            if meta_record:
                                meta_record["status"], meta_record["result"] = (
                                    "completed",
                                    outcome,
                                )
                        collected_results_ordered[original_chunk_index] = outcome
            except Exception as e_collect:
                for task_info in tasks_for_result_collection:
                    original_chunk_index = task_info["original_index"]
                    if collected_results_ordered[original_chunk_index] is None:
                        collected_results_ordered[original_chunk_index] = e_collect
                        meta_record = next(
                            (
                                m
                                for m in self._task_metadata
                                if m["id"] == task_info["id"]
                            ),
                            None,
                        )
                        if meta_record and meta_record["status"] == "submitted":
                            meta_record["status"], meta_record["error"] = (
                                "failed",
                                e_collect,
                            )

        final_results: List[Union[Any, Exception]] = []
        for item in collected_results_ordered:
            if item is None:
                final_results.append(
                    RuntimeError(
                        "Resultado inesperado 'None' para um chunk processado."
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
            if self.distribution_strategy
            else "None"
        )
        return f"<Master cloud_manager={cm_name}, distribution_strategy={ds_name}>"
