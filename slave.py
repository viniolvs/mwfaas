# master_slave_faas/slave.py (versão com classe)

import traceback
# from .exceptions import TaskExecutionError # Se definida


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
        # print(f"[Slave:__init__] Slave instanciado com config: {self.config}")

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


# Como um handler FaaS (que estaria, por exemplo, em cloud_manager.py) poderia usar esta classe:
#
# import cloudpickle
# from .slave import Slave # Assumindo que o handler está em um módulo que pode importar assim
# # Ou: from master_slave_faas.slave import Slave
#
# def actual_faas_handler(event, context):
#     # Etapas (simplificadas):
#     # 1. Obter user_function (desserializada) e data_chunk do 'event'
#     #    (a desserialização da função e dos dados ocorreria aqui)
#     # user_function = cloudpickle.loads(bytes.fromhex(event['serialized_function_hex']))
#     # data_chunk = event['data_chunk']
#
#     # slave_instance = Slave() # Ou com alguma config se necessário
#     # return slave_instance.execute_task(user_function, data_chunk)
