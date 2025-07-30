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
    now_utc   = datetime.now(pytz.utc)
    now_local = now_utc - timedelta(hours=6)  # UTC → UTC-6 → UTC-3 (BRT)
    now       = now_local.strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{now}] extraindo UPDATED_TIME > {last} até {now}")

    # 3) Extrai o delta
    df = extract_incremental(last, now)
    if df.empty:
        print("▶ nenhum registro novo ou atualizado")
    else:
        print(f"▶ upsert de {len(df)} registros")
        # **DEBUG**: mostra quais IDs serão upsertados
        print("▶ IDs extraídos para upsert:", df["ID"].tolist())
        # 4) Faz o UPSERT no Supabase
        upsert_bitrix_cards(df)
        # 5) Só depois de bem-sucedido, grava o novo watermark
        save_last_update(now)
        print(f"✔ watermark atualizado para {now}")

if __name__ == "__main__":
    main()
