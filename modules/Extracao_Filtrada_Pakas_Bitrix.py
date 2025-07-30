import ast
import pandas as pd
from pandas import DataFrame
from typing import Literal, Dict, List
from pydantic.dataclasses import dataclass
import requests
from requests.adapters import HTTPAdapter, Retry
import traceback
import datetime
from datetime import datetime, timedelta
import re
import os
from dotenv import load_dotenv
load_dotenv()

# Tokens
# BITRIX_HASH_USER = "22"
# BITRIX_HASH_ACESSO = "ok80wjow5agq1ytp"
# TOKEN_BI_CONNECTOR = "vy4ucBBd0LeZND1RQ05yWa11pq6Sx2DRbr"

BITRIX_HASH_USER   = os.environ["BITRIX_HASH_USER"]
BITRIX_HASH_ACESSO = os.environ["BITRIX_HASH_ACESSO"]
TOKEN_BI_CONNECTOR = os.environ["TOKEN_BI_CONNECTOR"]

# Copiado de utils.class_base.py
class Request:
    def __init__(self, headers: dict = None):
        self.headers = headers
        self.session = requests.Session()
        retries = Retry(total=5, backoff_factor=2)
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

    def _verify_response(self, response: object) -> None:
        if response.ok:
            return response
        else:
            print(f"Erro na requisição: {response}")
            return response

    def get(self, url: str, payload=None) -> object:
        try:
            response = self.session.request(
                "GET", url, headers=self.headers, params=payload
            )
        except requests.exceptions.RequestException:
            print(f"{traceback.format_exc()}")
            return None
        return self._verify_response(response)

    def post(self, url: str, data=None, json=None, params=None) -> object:
        try:
            response = self.session.request(
                "POST", url, headers=self.headers, data=data, json=json, params=params
            )
        except requests.exceptions.RequestException:
            print(f"{traceback.format_exc()}")
            return None
        return self._verify_response(response)

# Copiado de utils.class_bi_connector.py
class BiConnectorBx:
    def __init__(self, start_date=None, end_date=None):
        self.url = "https://crm.hub-bnk.com/bitrix/tools/biconnector/pbi.php"
        self.start_date = (
            start_date
            if start_date != None
            else (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        )
        self.end_date = (
            end_date
            if end_date != None
            else (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d")
        )

    def get_data_default(
        self,
        table: str,
        fields: list[dict] = None,
        dimensionsFilters: list[list[dict]] = None,
        headers: dict = {"Content-Type": "application/json"},
    ) -> list:
        """
        Consulta dados do Bitrix via BiConnector das tabelas do CRM.
        Documentação de tabelas BiConnector: https://helpdesk.bitrix24.com.br/open/16404556/

        Args:
            table (str): Nome da tabela do Bitrix que será consultada.
            fields (dict, optional): Campos selecionados para serem retornados na consulta.
            dimensionsFilters (dict, optional): Filtros para a consulta.
            headers (dict, optional): Headers que serão passados pela requisição.

        Returns:
            list: Retorna uma lista de listas com os dados da consulta. O index zero é o cabeçalho da tabela.
        """
        query_params = {"table": table}
        request_body = {
            "dateRange": {"startDate": self.start_date, "endDate": self.end_date},
            "key": TOKEN_BI_CONNECTOR,
            "fields": fields,
            "dimensionsFilters": dimensionsFilters,
        }

        headers = headers

        response = Request(headers=headers).post(
            self.url, params=query_params, json=request_body
        )

        json_response = response.json()

        return json_response

# Copiado de utils.class_extract_bitrix.py
request = Request(
    {"Content-Type": "application/json", "Cookie": "BITRIX_SM_SALE_UID=0; qmb=0."}
)

@dataclass
class BitrixCrm:
    """
    Classe para criação de strings de requisições Bitrix.
    """
    BITRIX_HASH_USER: str = BITRIX_HASH_USER
    btrx_hash_access: str = BITRIX_HASH_ACESSO
    url_base: str = f"https://crm.hub-bnk.com/rest/{BITRIX_HASH_USER}/{BITRIX_HASH_ACESSO}/"
    btrx_method: Literal["add", "update", "delete", "list", None] = None
    btrx_class: Literal[
        "company", "contact", "deal", "lead", "department", "item", "user", None
    ] = None
    entity_type_id: int | None = None
    category_id: int | None = None

    def generate_str_request_from_dict(self, dict_request: dict) -> str:
        """
        Cria uma string de request a partir de um dicionário contendo campos e valores.
        """
        fields_str = ""

        string_update = f"{self.btrx_class}.{self.btrx_method}?"
        if self.btrx_class in ["contact", "item"]:
            string_update = f"crm.{self.btrx_class}.{self.btrx_method}?"

        try:
            if self.btrx_class == "item":
                string_update = f"{string_update}entityTypeId={self.entity_type_id}&fields[categoryId]={self.category_id}&"

            if "ID" in dict_request:
                string_update = f"{string_update}&id={dict_request['ID']}&"
                del dict_request["ID"]

            if "UF_DEPARTMENT" in dict_request:
                i = 0
                for department in ast.literal_eval(dict_request["UF_DEPARTMENT"]):
                    string_update = f"{string_update}UF_DEPARTMENT[{i}]={department}&"
                    i += 1
                del dict_request["UF_DEPARTMENT"]

            if len(dict_request) > 0:
                if self.btrx_class in ["contact", "item"]:
                    for field_val, value in dict_request.items():
                        fields_str += f"fields[{field_val}]={value}&"
                else:
                    for field_val, value in dict_request.items():
                        fields_str += f"{field_val}={value}&"

                return string_update + fields_str[:-1]
            else:
                return string_update[:-1]
        except Exception as e:
            raise ValueError("Não foi possível criar a string de requisição.") from e

@dataclass
class BitrixSPA(BitrixCrm):
    """
    Classe base para SPAs (Smart Process Automation) no Bitrix.
    """
    btrx_class: Literal["item"] = "item"
    category_id: int
    entity_type_id: int

    def read(
        self,
        fields: list[dict] = None,
        dimensionsFilters: list[list[dict]] = None,
    ) -> DataFrame:
        """
        Reads the cards from Be.Smart SPA.
        """
        lista_items_data = BiConnectorBx().get_data_default(
            f"crm_dynamic_items_{self.entity_type_id}",
            fields=fields,
            dimensionsFilters=dimensionsFilters,
        )

        df_data_items = pd.DataFrame(lista_items_data[1:], columns=lista_items_data[0])
        df_data_items = df_data_items[df_data_items["CATEGORY_ID"] == self.category_id]
        return df_data_items

    def read_all_categories(
        self, fields: List[Dict] = None, dimensionsFilters: List[List[Dict]] = None
    ) -> pd.DataFrame:
        """
        Lê os dados de todas as categorias definidas e retorna um único DataFrame consolidado.
        """
        data_dict = {}
        for category in self.category_ids:
            self.category_id = category  # Atualiza a categoria atual
            df = self.read(fields=fields, dimensionsFilters=dimensionsFilters)
            data_dict[category] = df

        if not data_dict:
            return pd.DataFrame()

        return pd.concat(data_dict.values(), keys=data_dict.keys())

@dataclass
class BitrixFinanceiro(BitrixSPA):
    entity_type_id: Literal[179] = 179
    category_id: Literal[823] = 823

# Função para converter o horário bruto (UTC+6) para Brasília (BRT, UTC–3)
def convert_timezone(time_str):
    try:
        if not time_str or not time_str.strip():
            return None
        time_dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        # Subtrai 6 horas para ajustar de UTC+6 para UTC-3
        time_brasilia = time_dt - timedelta(hours=6)
        return time_brasilia.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        print(f"Erro ao converter o fuso horário: {e}")
        return None

def adjust_history_timezone(hist_str):
    """
    Subtrai 6 horas de todos os timestamps no formato DD/MM/YYYY HH:MM:SS
    encontrados dentro da string de histórico.
    """
    if not hist_str or pd.isna(hist_str):
        return hist_str

    def _repl(match):
        date_str = match.group(1)
        dt = datetime.strptime(date_str, "%d/%m/%Y %H:%M:%S")
        dt_adj = dt - timedelta(hours=6)
        return dt_adj.strftime("%d/%m/%Y %H:%M:%S")

    pattern = r"(\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2})"
    return re.sub(pattern, _repl, hist_str)

# Função para ler todos os dados dos cards do Bitrix
def extract_all_bitrix_data():
    # Configurar o BiConnector para o dia atual
    today = datetime.now().strftime("%Y-%m-%d")
    bi_connector = BiConnectorBx(start_date=today, end_date=today)

    # Ler os cards do pipeline de comissões (category_id=823)
    bitrix = BitrixFinanceiro(category_id=823)
    df_cards = bitrix.read()  # Lê todos os campos disponíveis inicialmente

    if df_cards.empty:
        print("Nenhum card encontrado no Bitrix.")
        return

    # Definir as colunas de interesse
    columns_of_interest = [
        "ID",
        "CREATED_TIME",
        "UPDATED_TIME",
        "ASSIGNED_BY_NAME",
        "STAGE_NAME",
        "UF_CRM_335_TIPO_COMISSAO",
        "UF_CRM_335_USUARIO_SOLICITANTE",
        "UF_CRM_335_ORIGEM_COMISSAO",
        "UF_CRM_335_DESCRICAO_PROBLEMA_COMISSOES",
        "MOVED_BY_NAME",
        "UF_CRM_335_AUT_HISTORICO",
        "UF_CRM_335_AUT_ETAPA_8",
        "UF_CRM_335_AUT_ETAPA_9",
        "UF_CRM_335_MOTIVOCONCLUIDO",
        "UF_CRM_335_OBSERVACAO_CONCLUIDO",
        "UF_CRM_335_CANCELAMENTO_MOTIVOS",
        "UF_CRM_335_OBSERVACAO_CANCELAMENTO",
        "UF_CRM_335_TIPO_OUTROS_PROB",
        "UF_CRM_335_NPS",
        "UF_CRM_335_FEEDBAC_NPS"
    ]

    # Verificar quais colunas de interesse estão disponíveis nos dados
    available_columns = [col for col in columns_of_interest if col in df_cards.columns]
    missing_columns = [col for col in columns_of_interest if col not in df_cards.columns]

    if missing_columns:
        print(f"\nAviso: As seguintes colunas não foram encontradas nos dados do Bitrix: {missing_columns}")
        print(f"Colunas disponíveis: {df_cards.columns.tolist()}")

    # Filtrar apenas as colunas disponíveis
    df_cards = df_cards[available_columns]

    # Converter os horários para o fuso de Brasília
    if 'CREATED_TIME' in df_cards.columns:
        df_cards['CREATED_TIME'] = df_cards['CREATED_TIME'].apply(convert_timezone)
    if 'UPDATED_TIME' in df_cards.columns:
        df_cards['UPDATED_TIME'] = df_cards['UPDATED_TIME'].apply(convert_timezone)
    if 'UF_CRM_335_AUT_ETAPA_9' in df_cards.columns:
        df_cards['UF_CRM_335_AUT_ETAPA_9'] = df_cards['UF_CRM_335_AUT_ETAPA_9'].apply(convert_timezone)
    if 'UF_CRM_335_AUT_ETAPA_8' in df_cards.columns:
        df_cards['UF_CRM_335_AUT_ETAPA_8'] = df_cards['UF_CRM_335_AUT_ETAPA_8'].apply(convert_timezone)
    if 'UF_CRM_335_AUT_HISTORICO' in df_cards.columns:
        df_cards['UF_CRM_335_AUT_HISTORICO'] = (
            df_cards['UF_CRM_335_AUT_HISTORICO']
            .apply(adjust_history_timezone)
        )


    # Exibir as colunas disponíveis
    print("\nColunas disponíveis nos dados do Bitrix:")
    print(df_cards.columns.tolist())

    # Exibir as primeiras linhas para inspeção
    print("\nPrimeiras 5 linhas dos dados:")
    print(df_cards.head())

    # Salvar os dados em um arquivo Excel
    output_path = "O:/Comissões/Leonardo Naar/Automacao_Bitrix_Dashboard/Bitrix_Comissoes_Dados_Filtrados.xlsx"
    try:
        df_cards.to_excel(output_path, index=False, engine='openpyxl')
        print(f"\nDados completos salvos em: {output_path}")
    except Exception as e:
        print(f"Erro ao salvar os dados: {e}")

# Executar a função principal
if __name__ == "__main__":
    extract_all_bitrix_data()