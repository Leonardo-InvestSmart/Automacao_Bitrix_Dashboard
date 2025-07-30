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
    adjust_history_timezone,
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
    dt = datetime.now(pytz.utc) - timedelta(days=1)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def save_last_update(value: str):
    supabase.table("ETL_CONTROL") \
      .upsert({"source_table":"BITRIX_CARDS","last_updated":value}, on_conflict="source_table") \
      .execute()

def extract_incremental(start_iso: str, end_iso: str) -> pd.DataFrame:
    bi = BiConnectorBx(start_date=start_iso.split("T")[0],
                       end_date=     end_iso.split("T")[0])
    # 1) Puxa todos os registros (sem filtro OR falho)
    raw = bi.get_data_default(
        table=f"crm_dynamic_items_{BitrixFinanceiro.entity_type_id}",
        fields=None
    )
    df = pd.DataFrame(raw[1:], columns=raw[0])
    df = df[df["CATEGORY_ID"] == BitrixFinanceiro.category_id]

    # 2) Parseia UPDATED_TIME em aware datetime (UTC+6) e converte para UTC
    df["UPDATED_TIME_dt"] = pd.to_datetime(
        df["UPDATED_TIME"],
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce"
    )
    server_tz = pytz.timezone("Asia/Yekaterinburg")  # ou o timezone correto do servidor Bitrix
    df["UPDATED_TIME_dt"] = (
        df["UPDATED_TIME_dt"]
          .dt.tz_localize(server_tz)
          .dt.tz_convert(pytz.UTC)
    )

    # 3) Converte start_iso/end_iso para UTC-aware (suporta offset “+00:00”)
    start_dt = datetime.fromisoformat(start_iso).astimezone(pytz.UTC)
    end_dt   = datetime.fromisoformat(end_iso).  astimezone(pytz.UTC)

    # 4) Filtra pelo intervalo UTC correto
    df = df[
        (df["UPDATED_TIME_dt"] > start_dt) &
        (df["UPDATED_TIME_dt"] <= end_dt)
    ]

    # 5) Agora sim ajusta as colunas para exibição e storage
    for col in ("UPDATED_TIME","CREATED_TIME","UF_CRM_335_AUT_ETAPA_8","UF_CRM_335_AUT_ETAPA_9"):
        if col in df.columns:
            df[col] = df[col].apply(convert_timezone)

    # 6) Ajuste de histórico (antes⇨depois)…
    orig = df.get("UF_CRM_335_AUT_HISTORICO", pd.Series(dtype=object))
    df["historic_before"] = orig.fillna("")
    if "UF_CRM_335_AUT_HISTORICO" in df.columns:
        df["UF_CRM_335_AUT_HISTORICO"] = (
            df["UF_CRM_335_AUT_HISTORICO"]
              .apply(adjust_history_timezone)
        )

    return df[[c for c in COLUMNS if c in df.columns]]