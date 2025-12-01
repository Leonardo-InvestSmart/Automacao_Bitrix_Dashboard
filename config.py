import streamlit as st
import os
from supabase import create_client
import toml
from dotenv import load_dotenv
load_dotenv()


# 1) Primeiro tenta ler do ambiente
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# 2) Se não encontrou, carrega do secrets do Streamlit (útil em dev/local)
if not SUPABASE_URL or not SUPABASE_KEY:
    secrets = toml.load(".streamlit/secrets.toml")
    SUPABASE_URL = secrets["SUPABASE_URL"]
    SUPABASE_KEY = secrets["SUPABASE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# 3) Azure / OAuth (mesma ideia do SmartC)
TENANT_ID     = os.getenv("AZURE_TENANT_ID")
CLIENT_ID     = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
EMAIL_USER    = os.getenv("EMAIL_USER")

# Se não veio por variável de ambiente, tenta .streamlit/secrets.toml
if not (TENANT_ID and CLIENT_ID and CLIENT_SECRET and EMAIL_USER):
    if secrets is None:
        secrets = toml.load(".streamlit/secrets.toml")
    TENANT_ID     = TENANT_ID     or secrets.get("AZURE_TENANT_ID")
    CLIENT_ID     = CLIENT_ID     or secrets.get("AZURE_CLIENT_ID")
    CLIENT_SECRET = CLIENT_SECRET or secrets.get("AZURE_CLIENT_SECRET")
    EMAIL_USER    = EMAIL_USER    or secrets.get("EMAIL_USER")