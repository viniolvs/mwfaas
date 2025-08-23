from typing import List, Any
from .distribution_strategy import DistributionStrategy


class SingleDistribuitionStrategy(DistributionStrategy):
    """
    Uma estratégia que não divide os dados. Em vez disso, trata
    todo o data_input como um único bloco de trabalho.
    """

    def split_data(self, data_input: Any, num_target_splits: int) -> List[Any]:
        """
        Retorna uma lista contendo o data_input original como seu único elemento.
        O num_target_splits é ignorado, pois sempre haverá apenas um chunk.
        """
        if num_target_splits < 1:
            raise ValueError(
                "É necessário pelo menos um worker para a SingleChunkStrategy."
            )
        return [data_input]
