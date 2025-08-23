# mwfaas/distribution.py

import abc


class DistributionStrategy(abc.ABC):
    """
    Classe base abstrata para estratégias de distribuição de dados.
    Define a interface que todas as estratégias de distribuição devem implementar.
    """

    @abc.abstractmethod
    def split_data(self, data_input: list, num_target_splits: int) -> list:
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


class DefaultDistributionStrategy(DistributionStrategy):
    """
    Uma estratégia padrão que divide os dados da forma mais equilibrada possível
    entre o número alvo de divisões. Assume que `data_input` é uma lista.
    """

    def split_data(self, data_input: list, num_target_splits: int) -> list:
        """
        Divide uma lista de itens de dados em `num_target_splits` blocos da forma
        mais equilibrada possível.

        Args:
            data_input: A lista de itens de dados.
            num_target_splits: O número desejado de blocos. Deve ser um inteiro positivo.
                               Este valor é tipicamente fornecido pelo `Master` após consulta
                               ao `CloudManager.get_target_parallelism()`.

        Returns:
            Uma lista de listas (blocos). Retorna uma lista vazia se `data_input` for vazio.
            Se `num_target_splits` for maior que o número de itens, cada item se torna
            seu próprio bloco, e os "splits" restantes (endpoints) receberão blocos vazios.

        Raises:
            ValueError: Se `num_target_splits` não for um inteiro positivo.
                        (O `Master` deve garantir que um valor válido seja passado).
        """
        if not isinstance(num_target_splits, int) or num_target_splits <= 0:
            raise ValueError("num_target_splits deve ser um inteiro positivo.")

        if not data_input:
            return []

        n = len(data_input)
        chunks = []

        # Lógica para distribuir N itens em K splits:
        # base_size = N // K
        # remainder = N % K
        # Os primeiros 'remainder' splits recebem 'base_size + 1' itens.
        # Os 'K - remainder' splits restantes recebem 'base_size' itens.

        base_size = n // num_target_splits
        remainder = n % num_target_splits
        current_pos = 0

        for i in range(num_target_splits):
            chunk_size = base_size + (1 if i < remainder else 0)
            chunk = data_input[current_pos : current_pos + chunk_size]
            chunks.append(chunk)
            current_pos += chunk_size

        return chunks
