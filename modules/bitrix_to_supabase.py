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
    "UF_CRM_335_APROVA_RESOLUCAO",
    "UF_CRM_335_DESCRICAO_PROBLEMA_COMISSOES", 
    "MOVED_BY_NAME", "UF_CRM_335_AUT_HISTORICO", "UF_CRM_335_AUT_ETAPA_8",
    "UF_CRM_335_AUT_ETAPA_9", "UF_CRM_335_MOTIVOCONCLUIDO",
    "UF_CRM_335_OBSERVACAO_CONCLUIDO", "UF_CRM_335_CANCELAMENTO_MOTIVOS",
    "UF_CRM_335_OBSERVACAO_CANCELAMENTO", "UF_CRM_335_TIPO_OUTROS_PROB",
    "UF_CRM_335_NPS", "UF_CRM_335_FEEDBAC_NPS",
]

def _extract_data(resp):
    """
    Torna compatível com supabase-py/postgrest diferentes:
    - objeto com atributo .data
    - dict já com 'data'
    - lista/dict direto
    - None
    """
    if resp is None:
        return None
    if hasattr(resp, "data"):
        return resp.data
    if isinstance(resp, dict):
        return resp.get("data", resp)
    return resp  # pode ser list/obj semelhante

def get_last_update() -> str:
    try:
        resp = (
            supabase.table("ETL_CONTROL")
            .select("last_updated")
            .eq("source_table", "BITRIX_CARDS")
            .maybe_single()
            .execute()
        )
    except Exception as e:
        # Se o client retornar exceção (ex.: método ausente),
        # cai para uma consulta "normal" que sempre funciona.
        resp = (
            supabase.table("ETL_CONTROL")
            .select("last_updated")
            .eq("source_table", "BITRIX_CARDS")
            .execute()
        )

    data = _extract_data(resp)

    # Quando vem lista (0, 1 ou N linhas)
    if isinstance(data, list):
        if len(data) == 1 and isinstance(data[0], dict) and "last_updated" in data[0]:
            return data[0]["last_updated"]
        else:
            data = None  # 0 ou >1 → trata como ausente

    # Quando já veio dict único
    if isinstance(data, dict) and data.get("last_updated"):
        return data["last_updated"]

    # Não existe linha ainda → semeia uma watermark segura (BRT -1 dia)
    br = pytz.timezone("America/Sao_Paulo")
    initial_dt = (datetime.now(br) - timedelta(days=1)).isoformat()

    supabase.table("ETL_CONTROL").upsert(
        {"source_table": "BITRIX_CARDS", "last_updated": initial_dt},
        on_conflict="source_table"
    ).execute()

    return initial_dt



def save_last_update(value: str):
    try:
        supabase.table("ETL_CONTROL").upsert(
            {"source_table": "BITRIX_CARDS", "last_updated": value},
            on_conflict="source_table"
        ).execute()
    except Exception as e:
        print(f"⚠ Não foi possível atualizar watermark: {e}")

def extract_incremental(start_iso: str, end_iso: str) -> pd.DataFrame:
    bi = BiConnectorBx()
    # 1) Puxa todos os registros (sem filtro OR falho)
    raw = bi.get_data_default(
        table=f"crm_dynamic_items_{BitrixFinanceiro.entity_type_id}",
        fields=None
    )
    df = pd.DataFrame(raw[1:], columns=raw[0])
    # exibe até 10 registros com ID e UPDATED_TIME para facilitar depuração
    # converter o UPDATED_TIME do servidor (UTC+6) para BRT antes do debug
    df["UPDATED_TIME_BRT"] = df["UPDATED_TIME"].apply(convert_timezone)

    print("DEBUG RAW registros brutos (ID & UPDATED_TIME_BRT):")
    print(df[["ID","UPDATED_TIME_BRT"]].tail(10).to_dict(orient="records"))
    print(f"Total de registros brutos: {len(df)}")
    df = df[df["CATEGORY_ID"] == BitrixFinanceiro.category_id]
    print(f"DEBUG Após filtro CATEGORY_ID: {len(df)} registros")

    # 2) Parseia UPDATED_TIME em aware datetime (UTC+6) e converte para UTC
    # 1) converter para Brasília
    df["UPDATED_TIME_BRT"] = df["UPDATED_TIME"].apply(convert_timezone)

    # 2a) parsear UPDATED_TIME_BRT → UPDATED_TIME_dt UTC-aware
    df["UPDATED_TIME_dt"] = pd.to_datetime(
        df["UPDATED_TIME_BRT"],
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce"
    ).dt.tz_localize(pytz.timezone("America/Sao_Paulo"))

    # 2b) parsear CREATED_TIME_BRT → CREATED_TIME_dt UTC-aware
    # (primeiro converta Created para BRT, igual ao Updated)
    df["CREATED_TIME_BRT"] = df["CREATED_TIME"].apply(convert_timezone)
    df["CREATED_TIME_dt"] = pd.to_datetime(
        df["CREATED_TIME_BRT"],
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce"
    ).dt.tz_localize(pytz.timezone("America/Sao_Paulo"))


    # 3) Converte start_iso/end_iso para BRT-aware
    brasil = pytz.timezone("America/Sao_Paulo")

    # start_iso → datetime. Se vier sem tzinfo, assume BRT
    start_dt = datetime.fromisoformat(start_iso)
    if start_dt.tzinfo is None:
        start_dt = brasil.localize(start_dt)
    else:
        start_dt = start_dt.astimezone(brasil)

    # idem para end_iso
    end_dt = datetime.fromisoformat(end_iso)
    if end_dt.tzinfo is None:
        end_dt = brasil.localize(end_dt)
    else:
        end_dt = end_dt.astimezone(brasil)

    print(f"DEBUG Intervalo BRT: {start_dt} até {end_dt}")

    mask_updated = (df["UPDATED_TIME_dt"] > start_dt) & (df["UPDATED_TIME_dt"] <= end_dt)
    mask_created = (df["CREATED_TIME_dt"] > start_dt) & (df["CREATED_TIME_dt"] <= end_dt)

    df = df[ mask_updated | mask_created ]
    print(f"DEBUG {len(df)} registros após filtro incremental; min: {df['UPDATED_TIME_dt'].min()}, max: {df['UPDATED_TIME_dt'].max()}")

    # 5) Agora sim ajusta as colunas para exibição e storage
    for col in ("UPDATED_TIME","CREATED_TIME","UF_CRM_335_AUT_ETAPA_8","UF_CRM_335_AUT_ETAPA_9"):
        if col in df.columns:
            df[col] = df[col].apply(convert_timezone)

    # 6) Ajuste de histórico (antes⇨depois)…
    # aplica o timezone-adjust apenas na coluna original, sem historic_before
    if "UF_CRM_335_AUT_HISTORICO" in df.columns:
        df["UF_CRM_335_AUT_HISTORICO"] = (
            df["UF_CRM_335_AUT_HISTORICO"].apply(adjust_history_timezone)
        )

    cols_to_return = [c for c in COLUMNS if c in df.columns]

    return df[cols_to_return]