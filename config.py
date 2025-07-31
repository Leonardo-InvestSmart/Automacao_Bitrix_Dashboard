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