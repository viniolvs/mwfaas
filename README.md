# mwfaas

- Crie um ambiente virtual para instalar as dependências:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Configurando um endpoint

- referência: [globus-compute-endpoint](https://globus-compute.readthedocs.io/en/2.6.0/endpoints.html)
- _Disponível apenas no linux_
- Caso necessário instale o `pipx`

```bash
python -m pip install --user pipx
```

- Instale o `globus-compute-endpoint`:

```bash
python3 -m pipx install globus-compute-endpoint
```

- Configure o endpoint e o inicie:

```bash
globus-compute-endpoint configure endpoint_name
globus-compute-endpoint start endpoint_name
```

- Siga o fluxo de autenticação do globus e autentique o endpoint com o token
- Os endpoints que serão utilizados devem ser configurados e autenticados na mesma conta do globus
