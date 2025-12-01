import streamlit as st
import os
from supabase import create_client
import toml
from dotenv import load_dotenv

load_dotenv()

# ------------------------------------------------------------------
# Controle de fonte de configuração (env primeiro, depois secrets)
# ------------------------------------------------------------------
secrets = None  # garante que a variável exista em qualquer fluxo

# 1) Primeiro tenta ler do ambiente
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# 2) Se não encontrou, carrega do secrets do Streamlit (útil em dev/local)
if not SUPABASE_URL or not SUPABASE_KEY:
    try:
        secrets = toml.load(".streamlit/secrets.toml")
    except FileNotFoundError:
        secrets = {}
    SUPABASE_URL = SUPABASE_URL or secrets.get("SUPABASE_URL")
    SUPABASE_KEY = SUPABASE_KEY or secrets.get("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# 3) Azure / OAuth (mesma ideia do SmartC)
TENANT_ID     = os.getenv("AZURE_TENANT_ID")
CLIENT_ID     = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
EMAIL_USER    = os.getenv("EMAIL_USER")

# Se não veio por variável de ambiente, tenta .streamlit/secrets.toml
if not (TENANT_ID and CLIENT_ID and CLIENT_SECRET and EMAIL_USER):
    if secrets is None:
        try:
            secrets = toml.load(".streamlit/secrets.toml")
        except FileNotFoundError:
            secrets = {}
    TENANT_ID     = TENANT_ID     or secrets.get("AZURE_TENANT_ID")
    CLIENT_ID     = CLIENT_ID     or secrets.get("AZURE_CLIENT_ID")
    CLIENT_SECRET = CLIENT_SECRET or secrets.get("AZURE_CLIENT_SECRET")
    EMAIL_USER    = EMAIL_USER    or secrets.get("EMAIL_USER")
