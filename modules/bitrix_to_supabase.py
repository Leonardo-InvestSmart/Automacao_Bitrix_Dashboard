import time
from datetime import datetime, timedelta
import pytz
import pandas as pd

from config import supabase
from modules.db import upsert_bitrix_cards
from modules.Extracao_Filtrada_Pakas_Bitrix import (
    BiConnectorBx,
    BitrixFinanceiro,
    convert_timezone,
)

TABLE_NAME = "BITRIX_CARDS"
COLUMNS = [
    "ID", "UPDATED_TIME", "CREATED_TIME", "ASSIGNED_BY_NAME", "STAGE_NAME", "UF_CRM_335_TIPO_COMISSAO",
    "UF_CRM_335_USUARIO_SOLICITANTE", "UF_CRM_335_ORIGEM_COMISSAO",
    "UF_CRM_335_DESCRICAO_PROBLEMA_COMISSOES", 
    "MOVED_BY_NAME", "UF_CRM_335_AUT_HISTORICO", "UF_CRM_335_AUT_ETAPA_8",
    "UF_CRM_335_AUT_ETAPA_9", "UF_CRM_335_MOTIVOCONCLUIDO",
    "UF_CRM_335_OBSERVACAO_CONCLUIDO", "UF_CRM_335_CANCELAMENTO_MOTIVOS",
    "UF_CRM_335_OBSERVACAO_CANCELAMENTO", "UF_CRM_335_TIPO_OUTROS_PROB",
    "UF_CRM_335_NPS", "UF_CRM_335_FEEDBAC_NPS",
]

def get_last_update() -> str:
    resp = supabase \
      .table("ETL_CONTROL") \
      .select("last_updated") \
      .eq("source_table", "BITRIX_CARDS") \
      .single() \
      .execute()

    if resp.data and resp.data.get("last_updated"):
        return resp.data["last_updated"]
    # fallback:
    tz = pytz.timezone("America/Sao_Paulo")
    dt = datetime.now(tz) - timedelta(days=1)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def save_last_update(value: str):
    supabase.table("ETL_CONTROL") \
      .upsert({"source_table":"BITRIX_CARDS","last_updated":value}, on_conflict="source_table") \
      .execute()

def extract_incremental(start_iso: str, end_iso: str) -> pd.DataFrame:
    bi = BiConnectorBx(start_date=start_iso.split("T")[0],
                       end_date=     end_iso.split("T")[0])
    raw = bi.get_data_default(
        table=f"crm_dynamic_items_{BitrixFinanceiro.entity_type_id}",
        fields=None,
        # filtra por UPDATED_TIME entre start e end:
        dimensionsFilters=[
          [
            {"field":"UPDATED_TIME","operator":">","value":start_iso},
            {"field":"UPDATED_TIME","operator":"<=","value":end_iso},
          ]
        ]
    )
    df = pd.DataFrame(raw[1:], columns=raw[0])
    df = df[df["CATEGORY_ID"] == BitrixFinanceiro.category_id]

    # converte timezones
    for col in ("UPDATED_TIME","CREATED_TIME","UF_CRM_335_AUT_ETAPA_8","UF_CRM_335_AUT_ETAPA_9"):
        if col in df.columns:
            df[col] = df[col].apply(convert_timezone)

    return df[[c for c in COLUMNS if c in df.columns]]