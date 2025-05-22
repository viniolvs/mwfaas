# scripts/configure_globus_compute.py

import argparse
import json
import sys
import re  # Importando o módulo re para expressões regulares

# Importa o Client para interagir com o serviço do Globus Compute
from globus_compute_sdk import Client


def logout_globus():
    """
    Realiza a desautenticação com o Globus Compute.
    """
    print("\n--- Desautenticação Globus ---")
    try:
        client = Client()
        client.logout()
        print("Desautenticação Globus bem-sucedida.")
    except Exception as e:
        print(f"Erro durante a desautenticação: {e}")


def authenticate_globus():
    """
    Realiza a autenticação interativa com o Globus Compute.
    O SDK do Globus Compute geralmente lida com o fluxo de autenticação
    de forma transparente ao criar o cliente.
    """
    print("\n--- Autenticação Globus ---")
    print("Siga as instruções para completar o processo de login.")
    try:
        # Tenta uma operação que exige autenticação para forçar o fluxo de login
        client = Client()
        client.version_check()  # Uma chamada leve para verificar a autenticação
        print("Autenticação Globus bem-sucedida.")
        return True
    except Exception as e:
        print(f"Erro durante a autenticação: {e}")
        print("Por favor, tente novamente ou verifique suas credenciais.")
        return False


def select_or_configure_endpoint():
    """
    Permite ao usuário selecionar um ou mais endpoints existentes
    ou o orienta a configurar um novo.
    Retorna uma lista de IDs de endpoints selecionados.
    """
    print("\n--- Configuração de Endpoint do Globus Compute ---")
    print("Listando seus endpoints existentes...")

    endpoints_data = []
    try:
        client = Client()
        # Coleta todos os endpoints e os armazena para exibição e seleção
        for i, ep in enumerate(client.get_endpoints()):
            endpoints_data.append(ep)
            print(f"  {i + 1} - Nome: {ep['name']} - ID: {ep['uuid']}")
    except Exception as e:
        print(f"Não foi possível listar os endpoints: {e}")
        print("Certifique-se de que está autenticado e o serviço está disponível.")
        return []

    if not endpoints_data:
        print("\nNenhum endpoint do Globus Compute encontrado associado à sua conta.")
        print("Você precisa configurar e iniciar um endpoint.")
        print("Para isso, siga os passos abaixo no seu terminal:")
        print(
            "  1. Instale a ferramenta de endpoint: pip install globus-compute-endpoint"
        )
        print(
            "  2. Configure um novo endpoint (substitua <NOME_DO_ENDPOINT> por um nome único):"
        )
        print("     globus-compute-endpoint configure <NOME_DO_ENDPOINT>")
        print("  3. Após a configuração, inicie o endpoint:")
        print("     globus-compute-endpoint start <NOME_DO_ENDPOINT>")
        print(
            "\nExecute este script novamente depois que seu endpoint estiver configurado e iniciado."
        )
        return []

    while True:
        user_input = input(
            "\nDigite o(s) número(s) dos endpoints que deseja configurar (separados por espaço, vírgula, etc., ou 'q' para sair): "
        ).strip()

        if user_input.lower() == "q":
            return []

        # Usa re.findall para encontrar todos os grupos de dígitos
        # Isso tratará "1,2", "1 2", "1; 2" etc. corretamente
        selected_numbers_str = re.findall(r"\d+", user_input)

        selected_numbers = []
        try:
            selected_numbers = [int(num) for num in selected_numbers_str]
        except ValueError:
            print("Entrada inválida. Por favor, digite apenas números ou 'q'.")
            continue

        selected_endpoint_ids = []
        all_valid = True
        for num in selected_numbers:
            index = num - 1  # Ajusta para índice base 0
            if 0 <= index < len(endpoints_data):
                selected_endpoint_ids.append(endpoints_data[index]["uuid"])
            else:
                print(
                    f"Número de endpoint inválido: {num}. Por favor, verifique e tente novamente."
                )
                all_valid = False
                break

        if all_valid and selected_endpoint_ids:
            print(f"Endpoint(s) selecionado(s): {', '.join(selected_endpoint_ids)}")
            return selected_endpoint_ids
        elif all_valid and not selected_endpoint_ids:
            print("Nenhum número de endpoint válido foi fornecido. Tente novamente.")
        # Se all_valid for False, o loop continua e pede nova entrada


# As funções save_config e main permanecem as mesmas
def save_config(endpoint_ids: list, config_path="master_slave_config.json"):
    """
    Salva os IDs dos endpoints do Globus Compute em um arquivo de configuração.
    """
    config = {"globus_compute_endpoint_ids": endpoint_ids}  # Alterado para lista
    try:
        with open(config_path, "w") as f:
            json.dump(config, f, indent=4)
        print(f"\nConfigurações salvas em {config_path}")
    except IOError as e:
        print(f"Erro ao salvar o arquivo de configuração {config_path}: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Script de configuração para integração com Globus Compute."
    )
    parser.add_argument(
        "--authenticate",
        action="store_true",
        help="Apenas realiza a autenticação com o Globus Compute.",
    )
    parser.add_argument(
        "--configure-endpoint",
        action="store_true",
        help="Ajuda a selecionar/configurar um endpoint do Globus Compute e salva o(s) ID(s).",
    )
    parser.add_argument(
        "--logout",
        action="store_true",
        help="Realiza a desautenticação com o Globus Compute.",
    )

    args = parser.parse_args()

    # O cliente é instanciado aqui, e o SDK irá lidar com a autenticação de forma lazy
    # quando for necessário.

    if args.authenticate:
        if authenticate_globus():
            print("Autenticação concluída.")
        else:
            print("Falha na autenticação.")
        return

    if args.configure_endpoint:
        # A função agora retorna uma lista de IDs
        endpoint_ids = select_or_configure_endpoint()
        if endpoint_ids:
            save_config(endpoint_ids)
            print("\nConfiguração do Globus Compute concluída com sucesso!")
            print(
                f"Sua biblioteca usará o(s) endpoint(s) ID(s): '{', '.join(endpoint_ids)}'."
            )
        else:
            print("\nConfiguração de endpoint não concluída.")
        return

    if args.logout:
        logout_globus()
        return

    # Se nenhum argumento específico for fornecido, orienta o usuário
    print(
        "Nenhum argumento fornecido. Use --authenticate,  --configure-endpoint ou --logout."
    )
    print("Para autenticar: python configure_globus_compute.py --authenticate")
    print(
        "Para configurar um endpoint: python configure_globus_compute.py --configure-endpoint"
    )
    print("Para desautenticar: python configure_globus_compute.py --logout")
    print(
        "\nPara configurar sua máquina como um endpoint do Globus Compute, siga as instruções:"
    )
    print("  1. python3 -m pip install globus-compute-endpoint")
    print("  2. globus-compute-endpoint configure")
    print("  3. globus-compute-endpoint start <NOME_DO_ENDPOINT>")
    print("Depois, execute este script com --configure-endpoint.")


if __name__ == "__main__":
    main()
