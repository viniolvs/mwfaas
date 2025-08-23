# mwfaas/cloud_manager.py

import abc
from typing import Any


class CloudManager(abc.ABC):
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
