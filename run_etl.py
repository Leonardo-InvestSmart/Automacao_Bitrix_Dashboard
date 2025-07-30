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
    last_iso = get_last_update()

    # 2) Marca o now para usar como próximo watermark (hora de Brasília, UTC-3)
    now_iso  = datetime.now(
               pytz.timezone("America/Sao_Paulo")
           ).astimezone(pytz.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{now_iso}] extraindo UPDATED_TIME > {last_iso} até {now_iso}")

    # 3) Extrai o delta
    start_filter = last_iso.replace("T", " ").rstrip("Z")
    end_filter   = now_iso.  replace("T", " ").rstrip("Z")

    df = extract_incremental(start_filter, end_filter)
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
        save_last_update(now_iso)
        print(f"✔ watermark atualizado para {now_iso}")

if __name__ == "__main__":
    main()
