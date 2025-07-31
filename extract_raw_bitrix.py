# extract_raw_bitrix.py

import pandas as pd
from datetime import datetime, timedelta
from modules.Extracao_Filtrada_Pakas_Bitrix import BiConnectorBx, BitrixFinanceiro

# Defina aqui o range que você quer inspecionar
START_DATE = "2025-01-01"  # AAAA-MM-DD
END_DATE   = "2025-07-31"  # AAAA-MM-DD

# Quais colunas do raw Bitrix você quer examinar?
COLUMNS_OF_INTEREST = [
    "ID", "CATEGORY_ID", "CREATED_TIME", "UPDATED_TIME",
    "ASSIGNED_BY_NAME", "STAGE_NAME", "UF_CRM_335_MOTIVOCONCLUIDO",
    "UF_CRM_335_AUT_HISTORICO",
    "UF_CRM_335_AUT_ETAPA_8", "UF_CRM_335_AUT_ETAPA_9",
    # adicione aqui qualquer outra UF_CRM_* que queira ver
]

def extract_raw_bitrix(start_date: str, end_date: str) -> pd.DataFrame:
    bi = BiConnectorBx(start_date=start_date, end_date=end_date)
    raw = bi.get_data_default(
        table=f"crm_dynamic_items_{BitrixFinanceiro.entity_type_id}",
        fields=None,
        dimensionsFilters=None  # sem filtro de UPDATED_TIME no BiConnector
    )
    # Monta DataFrame e filtra apenas a categoria financeira
    df = pd.DataFrame(raw[1:], columns=raw[0])
    df = df[df["CATEGORY_ID"] == BitrixFinanceiro.category_id]
    # Seleciona só as colunas que queremos inspecionar (ou todas, se preferir)
    available = [c for c in COLUMNS_OF_INTEREST if c in df.columns]
    return df[available]

if __name__ == "__main__":
    df = extract_raw_bitrix(START_DATE, END_DATE)
    print(f"Total de registros extraídos: {len(df)}")
    print("Colunas disponíveis:", df.columns.tolist())
    print("\nPrimeiras 5 linhas:")
    print(df.head().to_string(index=False))

    # Salva para Excel e CSV, para você abrir no Excel/Power BI etc
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    df.to_excel(f"raw_bitrix_{now}.xlsx", index=False, engine="openpyxl")
    df.to_csv (f"raw_bitrix_{now}.csv",  index=False)
    print(f"\nArquivos gravados em raw_bitrix_{now}.xlsx/.csv")
