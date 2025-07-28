import pandas as pd
import numpy as np
import math
import json
from postgrest import APIError
from config import supabase

def upsert_bitrix_cards(df) -> None:
    if isinstance(df, list):
        df = pd.DataFrame(df)
    df_clean = df.replace({np.inf: np.nan, -np.inf: np.nan})
    df_clean = df_clean.where(pd.notnull(df_clean), None)

    # Datas → ISO-8601 / limpa NaT→None
    for col in ("CREATED_TIME", "UPDATED_TIME", "UF_CRM_335_AUT_ETAPA_8", "UF_CRM_335_AUT_ETAPA_9"):
        if col in df_clean.columns:
            if col == "CREATED_TIME":
                parsed = pd.to_datetime(df_clean[col],
                                        format="%Y-%m-%d %H:%M:%S",
                                        errors="coerce")
            else:
                parsed = pd.to_datetime(df_clean[col],
                                        dayfirst=True,
                                        errors="coerce")
            iso = parsed.dt.tz_localize(None).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            df_clean[col] = iso.where(iso.notna(), None)

    # **3) Convertendo NPS para int puro**
    if "UF_CRM_335_NPS" in df_clean.columns:
        df_clean["UF_CRM_335_NPS"] = pd.to_numeric(
            df_clean["UF_CRM_335_NPS"], errors="coerce"
        ).apply(lambda v: int(v) if (v is not None and not math.isnan(v)) else None)

    # Floats/ints finitos → ok, senão None
    for col in df_clean.select_dtypes(include=["float64", "float32", "int64"]).columns:
        df_clean[col] = df_clean[col].apply(
            lambda v: None
            if (v is None or not isinstance(v, (int, float)) or not math.isfinite(v))
            else v
        )

    # numpy scalars → Python; "" → None
    records = []
    for rec in df_clean.to_dict(orient="records"):
        clean_rec = {}
        for k, v in rec.items():
            if isinstance(v, np.generic):
                v = v.item()
            elif isinstance(v, str) and v.strip() == "":
                v = None
            clean_rec[k] = v
        records.append(clean_rec)

    if not records:
        print("Nenhum registro válido para upsert.")
        return

    # Remove qualquer float não finito restante
    for rec in records:
        nps = rec.get("UF_CRM_335_NPS")
        if isinstance(nps, float):
            # se for inteiro sem resto, mantém como int
            if math.isfinite(nps) and nps.is_integer():
                rec["UF_CRM_335_NPS"] = int(nps)
            else:
                rec["UF_CRM_335_NPS"] = None

    # Teste de serialização JSON
    try:
        json.dumps(records, allow_nan=False)
    except ValueError:
        print("❌ Falha JSON! Primeiro registro problemático:")
        for bad in records:
            try:
                json.dumps(bad, allow_nan=False)
            except ValueError:
                print(json.dumps(bad, default=str, indent=2))
                break
        raise

    # Upsert em chunks
    chunk_size = 100
    for i in range(0, len(records), chunk_size):
        chunk = records[i : i + chunk_size]
        try:
            supabase.table("BITRIX_CARDS") \
                     .upsert(chunk, on_conflict="ID") \
                     .execute()
            print(f"✅ Upsert OK registros {i}–{i+len(chunk)-1}")
        except APIError as e:
            print(f"❌ Erro no chunk {i}–{i+len(chunk)-1}: {e}")
            for single in chunk:
                try:
                    supabase.table("BITRIX_CARDS") \
                             .upsert([single], on_conflict="ID") \
                             .execute()
                except APIError as ex:
                    print(f"    • Falha no ID {single.get('ID')}: {ex}")
