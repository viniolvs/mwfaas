# mwfaas/distribution.py

import abc
from typing import List, Any


class DistributionStrategy(abc.ABC):
    """
    Classe base abstrata para estratégias de distribuição de dados.
    Define a interface que todas as estratégias de distribuição devem implementar.
    """

    @abc.abstractmethod
    def split_data(self, data_input: Any, num_target_splits: int) -> List:
        """
        Divide os dados de entrada em um número especificado de blocos (chunks).

        Args:
            data_input: Uma lista de itens de dados a serem divididos.
            num_target_splits: O número desejado de blocos de dados. Espera-se que seja
                               um inteiro positivo.

        Returns:
            Uma lista de listas, onde cada lista interna é um bloco (chunk) dos dados originais.
            Deve retornar uma lista vazia se `data_input` for vazio.
        """
        pass
