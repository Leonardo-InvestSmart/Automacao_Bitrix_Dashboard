# full_load_bitrix.py

from datetime import datetime
import pytz

from modules.bitrix_to_supabase import extract_incremental
from modules.db                  import upsert_bitrix_cards


def main():

    start_iso = "2020-01-01 00:00:00"
    brazil_tz = pytz.timezone("America/Sao_Paulo")
    end_iso = datetime.now(brazil_tz).strftime("%Y-%m-%d %H:%M:%S")

    print(f"[FULL LOAD] Extraindo de {start_iso} até {end_iso}…")
    df = extract_incremental(start_iso, end_iso)
    print(f"> Total de registros encontrados: {len(df)}")

    if df.empty:
        print("⚠️ Não há registros para carregar.")
        return

    print("▶ Iniciando upsert de todos os registros…")
    upsert_bitrix_cards(df)
    print("✅ Full load concluído.")

if __name__ == "__main__":
    main()
