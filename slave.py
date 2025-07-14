# msfaas/slave.py


class Slave:
    """
    Representa um trabalhador escravo que executa uma determinada função em um bloco de dados.
    Esta classe é projetada para ser instanciada e usada dentro de um ambiente FaaS.
    """

    def __init__(self, config=None):
        """
        Inicializa o Slave.

        Args:
            config (dict, optional): Configurações para o escravo. Padrão para None.
                                     Poderia ser usado se o escravo precisasse de alguma
                                     configuração no momento da instanciação.
        """
        self.config = config

    def execute_task(self, user_function, data_chunk):
        """
        Executa a função fornecida pelo usuário com o respectivo pedaço de dados.

        Args:
            user_function (callable): A função que o usuário definiu para processar os dados.
            data_chunk (any): A porção dos dados que esta instância do escravo deve processar.

        Returns:
            any: O resultado da execução de `user_function(data_chunk)`.

        Raises:
            Exception: Propaga qualquer exceção levantada por `user_function`.
        """
        try:
            # print(f"[Slave:execute_task] Executando tarefa.")
            result = user_function(data_chunk)
            # print(f"[Slave:execute_task] Tarefa concluída.")
            return result
        except Exception as e:
            # print(f"[Slave:execute_task] Erro durante a execução da tarefa: {str(e)}")
            # print(f"[Slave:execute_task] Traceback:\n{traceback.format_exc()}")
            # Se TaskExecutionError fosse usado:
            # raise TaskExecutionError(f"Erro no escravo ao executar tarefa: {str(e)}") from e
            raise
