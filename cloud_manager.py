# mwfaas/cloud_manager.py

import abc
from concurrent.futures import Future  # Importa a classe Future
from typing import Any, List


class CloudManager(abc.ABC):
    """
    Classe base abstrata para Gerenciadores de Nuvem (Cloud Managers).
    Define a interface para interagir com um backend FaaS (Function as a Service).
    """

    @abc.abstractmethod
    def get_worker_count(self) -> int:
        """
        Retorna o número recomendado ou disponível de workers/endpoints paralelos.
        Este valor influencia como a carga de trabalho é dividida.
        """
        pass

    @abc.abstractmethod
    def get_active_worker_ids(self) -> List[str]:
        """
        Retorna uma lista de workers/endpoints disponíveis.
        """
        pass

    @abc.abstractmethod
    def submit_task(
        self, worker_id: str, serialized_function_bytes: bytes, data_chunk: Any
    ) -> Future:
        """
        Submete uma tarefa (uma função serializada e um bloco de dados) para execução.

        Args:
            serialized_function_bytes: A função do usuário, serializada (por exemplo, com cloudpickle).
            data_chunk: O bloco de dados para esta tarefa.

        Returns:
            Uma instância de Future que representa a tarefa.
        """
        pass

    @abc.abstractmethod
    def get_results_for_ids(
        self, task_ids: List, timeout_per_task: float | None = None
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
        pass

    def __enter__(self):
        """Permite o uso como gerenciador de contexto (context manager)."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Garante que shutdown() seja chamado ao sair do contexto."""
        self.shutdown()
