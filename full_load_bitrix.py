# full_load_bitrix.py

from datetime import datetime
import pytz

from modules.bitrix_to_supabase import extract_incremental
from modules.db                  import upsert_bitrix_cards

def main():
    # 1) Defina o timezone de SP
    tz = pytz.timezone("America/Sao_Paulo")

    # 2) Intervalo completo: de 01/01/2020 até agora
    start_iso = "2020-01-01 00:00:00"
    end_iso   = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

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
