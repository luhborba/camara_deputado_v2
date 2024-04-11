"""Arquivo de extrair dados da API da Camara dos Deputados."""

import os
import zipfile
from datetime import datetime
from typing import List

import duckdb
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

MD_TOKEN = os.getenv("MD_TOKEN")

url = "https://dadosabertos.camara.leg.br/api/v2/deputados"


def conectando_mortheduck():
    try:
        con = duckdb.connect(f"md:camara_deputados?motherduck_token={MD_TOKEN}")
        print("Conexão bem-sucedida!!!")
        return con
    except Exception as e:
        print(f"Error de Conexão: {e}")


def receber_lista_id_deputados():
    """Receber lista de id deputados."""
    response = requests.get(url)

    try:
        if response.status_code == 200:
            data = response.json()
            deputados_id = [deputado["id"] for deputado in data["dados"]]
            return deputados_id
    except Exception as e:
        print(f"Error ao se conectart com API:{e}")
        return None


def dados_deputados_por_id(deputados_id: List[int]):
    """
    Receber dados deputados por id.

    Args:
        deputados_id (List[int]): Lista de id deputados.
    """
    response = requests.get(f"{url}/{deputados_id}")

    try:
        if response.status_code == 200:
            data = response.json()
            return data["dados"]
    except Exception as e:
        print(f"Error ao se conectart com API:{e}")
        return None


def capturando_dados_deputados(conexao):
    """
    Função responsável por capturar dados de conexão e enviar para Banco de Dados.

    Args:
        con (duckdb.connect): Conexão com o banco de dados.
    """
    dados = []
    deputados_id = receber_lista_id_deputados()

    for id in deputados_id:
        dados_deputados = dados_deputados_por_id(id)
        if dados_deputados:
            dado = {
                "id": dados_deputados["id"],
                "nome": dados_deputados["ultimoStatus"]["nome"],
                "idLegislatura": dados_deputados["ultimoStatus"]["idLegislatura"],
                "siglaUF": dados_deputados["ultimoStatus"]["siglaUf"],
                "siglaPartido": dados_deputados["ultimoStatus"]["siglaPartido"],
                "nomeEleitoral": dados_deputados["ultimoStatus"]["nomeEleitoral"],
                "situacao": dados_deputados["ultimoStatus"]["situacao"],
                "sexo": dados_deputados["sexo"],
                "escolaridade": dados_deputados["escolaridade"],
                "dataNascimento": dados_deputados["dataNascimento"],
                "dataEnvio": datetime.now(),
            }
            dados.append(dado)

    def salvar_dados_duckdb(dados, conexao):
        conexao.execute("DROP TABLE IF EXISTS deputados")
        conexao.execute(
            "CREATE TABLE IF NOT EXISTS deputados (id INTEGER, nome VARCHAR, idLegislatura INTEGER, siglaUF VARCHAR, siglaPartido VARCHAR, nomeEleitoral VARCHAR, situacao VARCHAR, sexo VARCHAR, escolaridade VARCHAR, dataNascimento DATETIME, dataEnvio DATETIME)"
        )

        # Inserir os dados na tabela
        for dado in dados:
            conexao.execute(
                "INSERT INTO deputados VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    dado["id"],
                    dado["nome"],
                    dado["idLegislatura"],
                    dado["siglaUF"],
                    dado["siglaPartido"],
                    dado["nomeEleitoral"],
                    dado["situacao"],
                    dado["sexo"],
                    dado["escolaridade"],
                    dado["dataNascimento"],
                    dado["dataEnvio"],
                ),
            )

        print(
            f"Dados salvos com sucesso na tabela 'deputados' com: {len(dados)} registros."
        )

    salvar_dados_duckdb(dados, conexao)


def capturando_dados_gastos(conexao):
    url_gastos_2024 = "https://www.camara.leg.br/cotas/Ano-2024.csv.zip"
    ult_gastos_2023 = "https://www.camara.leg.br/cotas/Ano-2023.csv.zip"

    diretorio = "data/"
    print("Iniciando Extração de Gastos dos Deputados...")

    def baixar_arquivos_zip(url, diretorio):
        response = requests.get(url)

        if response.status_code == 200:
            if not os.path.exists(diretorio):
                os.makedirs(diretorio)

            arquivo_zip = url.split("/")[-1]

            caminho_arquivo_zip = os.path.join(diretorio, arquivo_zip)

            with open(caminho_arquivo_zip, "wb") as f:
                f.write(response.content)

            with zipfile.ZipFile(caminho_arquivo_zip, "r") as zip_ref:
                zip_ref.extractall(diretorio)

            print(f"Arquivo '{arquivo_zip}' baixado e extraído com sucesso!")
            os.remove(caminho_arquivo_zip)
        else:
            print(
                f"Falha ao baixar o arquivo na url {url}: Código de status: {response.status_code}"
            )

    baixar_arquivos_zip(url_gastos_2024, diretorio)
    baixar_arquivos_zip(ult_gastos_2023, diretorio)

    print("Iniciando Envio dos Dados de Gastos dos Deputados...")

    def enviar_dados_gastos_duckdb(conexao):
        try:
            conexao.execute("DROP TABLE IF EXISTS gastos")
            conexao.sql('CREATE TABLE gastos AS SELECT * FROM "./data/*.csv"')
            print("Dados de Gastos enviados com sucesso!!!")
        except Exception as e:
            print(f"Error ao se conectart com API:{e}")

    enviar_dados_gastos_duckdb(conexao)


if __name__ == "__main__":
    conexao = conectando_mortheduck()
    # capturando_dados_deputados(conexao)
    capturando_dados_gastos(conexao)
