# mwfaas/cli/list_globus_endpoints.py

import os
import json
from typing import List, Dict, Any, Set

from ..globus_compute_manager import GlobusComputeCloudManager


def load_configured_ids(config_path: str) -> Set[str]:
    """
    Lê o arquivo de configuração JSON e retorna um conjunto com os UUIDs
    dos endpoints que já foram configurados.

    Args:
        config_path: O caminho para o arquivo globus_config.json.

    Returns:
        Um conjunto (set) de strings contendo os UUIDs configurados.
    """
    if not os.path.exists(config_path):
        return set()

    try:
        with open(config_path, "r") as f:
            config_data = json.load(f)

        endpoints = config_data.get("globus_compute_endpoints", [])

        return {ep.get("id") for ep in endpoints if ep.get("id")}

    except (json.JSONDecodeError, IOError) as e:
        print(
            f"\nAviso: Não foi possível ler ou decodificar o arquivo de configuração '{config_path}': {e}"
        )
        return set()


def display_endpoints_table(endpoints: List[Dict[str, Any]], configured_ids: Set[str]):
    """Formata e exibe a lista de endpoints como uma tabela, incluindo a coluna 'CONFIGURADO'."""
    if not endpoints:
        print("\nNenhum endpoint encontrado registrado na sua conta.")
        print(
            "Para configurar um, siga as instruções da documentação do Globus Compute."
        )
        return

    line_size = 105
    print("\n" + "-" * line_size)
    print(f"{'NOME DO ENDPOINT':<30} {'STATUS':<15} {'CONFIGURADO':<15} {'ID'}")
    print("-" * line_size)

    for ep in endpoints:
        status = ep["status"]
        if "ERRO" in status:
            status_colored = f"\033[91m{status.upper()}\033[0m"  # Vermelho
        elif status == "online":
            status_colored = f"\033[92m{status.upper()}\033[0m"  # Verde
        elif status == "offline":
            status_colored = f"\033[91m{status.upper()}\033[0m"  # Vermelho
        else:
            status_colored = f"\033[93m{status.upper()}\033[0m"  # Amarelo

        is_configured_str = "Sim" if ep["uuid"] in configured_ids else "Não"
        print(
            f"{ep['name']:<30} {status_colored:<24} {is_configured_str:<15} {ep['uuid']}"
        )

    print("-" * line_size)


def main():
    """Ponto de entrada principal para o script de listagem de endpoints."""
    try:
        project_root = os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
        config_file_path = os.path.join(project_root, "globus_config.json")
    except NameError:
        config_file_path = os.path.join("..", "globus_config.json")

    configured_ids = load_configured_ids(config_file_path)
    available_endpoints = GlobusComputeCloudManager.list_endpoints()
    display_endpoints_table(available_endpoints, configured_ids)


if __name__ == "__main__":
    main()
