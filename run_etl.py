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
    try:
        last = get_last_update()
        brasil    = pytz.timezone("America/Sao_Paulo")
        now_local = datetime.now(brasil)
        # isoformat inclui automaticamente "-03:00"
        now       = now_local.isoformat()
        print(f"[{now}] extraindo UPDATED_TIME > {last} até {now}")

        df = extract_incremental(last, now)
        print(f"DEBUG ETL: DataFrame incremental com {len(df)} registros")
        if df.empty:
            print("▶ nenhum registro novo ou atualizado")
        else:
            print(f"▶ upsert de {len(df)} registros")
            print("▶ IDs extraídos:", df["ID"].tolist())
            try:
                upsert_bitrix_cards(df)
            except Exception as e:
                print("❌ Upsert falhou, watermark não será atualizado.")
                raise
            else:
                save_last_update(now)
                print(f"✔ watermark atualizado para {now}")
    except Exception as e:
        import traceback; traceback.print_exc()
        print("❌ Erro durante ETL:", e)

if __name__ == "__main__":
    main()

