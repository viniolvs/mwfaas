from typing import List, Any
from .distribution_strategy import (
    DistributionStrategy,
)


class ListDistributionStrategy(DistributionStrategy):
    """
    Uma estratégia que divide uma lista de itens em blocos (chunks)
    de um tamanho configurável.
    """

    def __init__(self, items_per_chunk: int = 1):
        """
        Inicializa a estratégia de distribuição.

        Args:
            items_per_chunk (int, optional): O número de itens da lista de entrada
                                             que devem ser agrupados em cada chunk.
                                             O padrão é 1, o que cria uma tarefa
                                             para cada item (máximo paralelismo).
        """
        if not isinstance(items_per_chunk, int) or items_per_chunk <= 0:
            raise ValueError("items_per_chunk deve ser um inteiro positivo.")
        self.items_per_chunk = items_per_chunk

    def split_data(
        self, data_input: List[Any], num_target_splits: int
    ) -> List[List[Any]]:
        """
        Divide a lista de entrada em blocos com `items_per_chunk` em cada um.

        Nota: O parâmetro `num_target_splits` é ignorado por esta estratégia,
        pois a divisão é baseada no tamanho do chunk, não no número de workers.

        Args:
            data_input: A lista de itens de dados.
            num_target_splits: Ignorado por esta implementação.

        Returns:
            Uma lista de listas (blocos).
        """
        if not data_input:
            return []

        # A lógica agora é muito mais simples: fatiar a lista em pedaços de tamanho fixo.
        chunks = []
        for i in range(0, len(data_input), self.items_per_chunk):
            chunk = data_input[i : i + self.items_per_chunk]
            chunks.append(chunk)

        return chunks
