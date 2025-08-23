# docs/generate_architecture_diagram.py

from diagrams import Diagram, Cluster, Edge
from diagrams.programming.language import Python
from diagrams.onprem.compute import Server
from diagrams.onprem.client import User
from diagrams.custom import Custom

# Defina os atributos gráficos para os clusters e nós para manter a consistência.
graph_attr = {
    "fontsize": "16",
    "bgcolor": "#F7F7F7",
    "splines": "ortho",
}

cluster_attr = {
    "fontsize": "14",
    "fontcolor": "#333333",
    "style": "rounded",
    "bgcolor": "#F7F7F7",
}

node_attr = {
    "fontsize": "12",
    "fontname": "Arial",
}

with Diagram(
    "Arquitetura Master-Slave FaaS com Globus Compute",
    show=False,  # Não abre a imagem automaticamente após gerar
    filename="arquitetura_mwfaas",
    direction="TB",  # TB = Top to Bottom
    graph_attr=graph_attr,
    node_attr=node_attr,
):
    # Elementos Externos
    user = User("Usuário da Biblioteca")

    with Cluster("Entradas do Usuário", graph_attr=cluster_attr):
        user_function = Python("user_function")
        data_input = Custom("Dados de Entrada", "./data_icon.png")

    # Cluster principal da sua biblioteca
    with Cluster("Biblioteca mwfaas", graph_attr=cluster_attr):
        master = Python("Master")
        distribution_strategy = Python("Distribution Strategy")

        # O Cloud Manager atua como uma ponte para a infraestrutura externa.
        # Estamos usando um ícone customizado para Globus, mas você poderia usar um genérico.
        cloud_manager = Custom("GlobusCompute Manager", "./globus_icon.png")

        # Relações internas da biblioteca
        master >> Edge(label="usa") >> distribution_strategy
        master >> Edge(label="chama") >> cloud_manager

    # Cluster representando a infraestrutura de execução remota
    with Cluster("Infraestrutura Globus Compute", graph_attr=cluster_attr):
        globus_endpoint = Server("Globus Compute Endpoint")

        with Cluster("Nós de Trabalho (Workers)"):
            workers = [
                Python("Slave (worker 1)"),
                Python("Slave (worker 2)"),
                Python("Slave (worker N...)"),
            ]

        # Relações na infraestrutura
        cloud_manager >> Edge(label="submete tarefa") >> globus_endpoint
        globus_endpoint >> Edge(label="executa em") >> workers[0]

    # Fluxo de dados principal
    user >> Edge(label="fornece") >> user_function
    user >> Edge(label="fornece") >> data_input
    data_input >> Edge(label="processar") >> master
    user_function >> Edge(label="executar") >> master

    (
        cloud_manager
        >> Edge(color="darkgreen", style="dashed", label="retorna resultados")
        >> master
    )
    (
        master
        >> Edge(color="darkgreen", style="dashed", label="retorna resultados")
        >> user
    )
