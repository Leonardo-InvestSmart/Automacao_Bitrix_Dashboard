import time
from datetime import datetime
import pytz
from config import supabase
from modules.db import upsert_bitrix_cards
from modules.bitrix_to_supabase import (
    get_last_update,
    save_last_update,
    extract_incremental,
)

def main():
    tz = pytz.timezone("America/Sao_Paulo")
    # 1) Lê o último UPDATED_TIME processado
    last = get_last_update()  # ex: "2025-07-22T12:00:00Z"
    # 2) Marca o now para usar como próximo watermark
    now = datetime.now(tz).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{now}] extraindo UPDATED_TIME > {last} até {now}")

    # 3) Extrai o delta
    df = extract_incremental(last, now)
    if df.empty:
        print("▶ nenhum registro novo ou atualizado")
    else:
        print(f"▶ upsert de {len(df)} registros")
        # 4) Faz o UPSERT no Supabase
        upsert_bitrix_cards(df.to_dict("records"))
        # 5) Só depois de bem-sucedido, grava o novo watermark
        save_last_update(now)
        print(f"✔ watermark atualizado para {now}")

if __name__ == "__main__":
    main()
