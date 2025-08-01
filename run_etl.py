import time
from datetime import datetime, timedelta
import pytz
from config import supabase
from modules.db import upsert_bitrix_cards
from modules.bitrix_to_supabase import (
    get_last_update,
    save_last_update,
    extract_incremental,
) 

def main():
    # 1) Lê o último UPDATED_TIME processado
    last = get_last_update()

    # 2) Marca o now para usar como próximo watermark (hora de Brasília, UTC-3)
    brazil_tz = pytz.timezone("America/Sao_Paulo")
    now_local = datetime.now(brazil_tz)
    now       = now_local.strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{now}] extraindo UPDATED_TIME > {last} até {now}")

    # 3) Extrai o delta
    df = extract_incremental(last, now)
    if df.empty:
        print("▶ nenhum registro novo ou atualizado")
    else:
        print(f"▶ upsert de {len(df)} registros")
        print("▶ IDs extraídos para upsert:", df["ID"].tolist())

        cols = ["ID"]
        if "historic_before" in df.columns and "UF_CRM_335_AUT_HISTORICO" in df.columns:
            cols += ["historic_before", "UF_CRM_335_AUT_HISTORICO"]

        preview = df[cols]
        if len(cols) > 1:
            print("▶ Histórico (antes ⇒ depois):")
            print(preview.to_dict(orient="records"))
        # 4) Faz o UPSERT no Supabase
        upsert_bitrix_cards(df)
        # 5) Só depois de bem-sucedido, grava o novo watermark
        save_last_update(now)
        print(f"✔ watermark atualizado para {now}")

if __name__ == "__main__":
    main()
