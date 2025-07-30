import streamlit as st
import pandas as pd
import plotly.express as px
from config import supabase
from streamlit_autorefresh import st_autorefresh

# --- Configuração da página ---
st.set_page_config(
    page_title="Gestão à Vista – Comissões",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Atualiza a cada 5 minutos (300.000 milissegundos)
st_autorefresh(interval=300000, key="refresh")

import datetime
st.markdown(f"🕒 Última atualização: `{datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')}`")

# --- CSS para cards e estilo geral ---
st.markdown("""
<style>
/* Container de métricas */
.box-container {
  display: flex;
  gap: 1rem;
}
/* Cada card */
.metric-box {
  flex: 1;
  background-color: #222222;
  border-radius: 8px;
  padding: 1rem;
  text-align: center;
  color: white;
}
.metric-box h3 {
  margin: 0;
  font-size: 2rem;
}
.metric-box p {
  margin: 0;
  font-size: 0.9rem;
  color: #888888;
  text-transform: uppercase;
}
/* Ajusta título */
h1 {
  margin-bottom: 0.2rem;
}
</style>
""", unsafe_allow_html=True)

# --- Helper para carregar views ---
@st.cache_data
def load_data(view_name: str) -> pd.DataFrame:
    resp = supabase.table(view_name).select("*").execute()
    return pd.DataFrame(resp.data)

# Carrega a view principal e a tabela completa para clientes/internos
df_ov = load_data("KPIS_OVERVIEW").iloc[0]
df_all = load_data("BITRIX_CARDS")

# Computa “CLIENTES” e “INTERNOS” automaticamente
total_clientes = int(df_all["UF_CRM_335_ORIGEM_COMISSAO"].eq("Cliente").sum())
total_internos = int(df_all["UF_CRM_335_ORIGEM_COMISSAO"].eq("Interno").sum())


# --- HEADER ---
c1, c2 = st.columns([4,1])
with c1:
    st.markdown(
        "<h1 style='color:#FFF'>GESTÃO À VISTA – "
        "<span style='color:#A259FF'>COMISSÕES</span></h1>",
        unsafe_allow_html=True
    )
with c2:
    st.image("assets/investsmart_horizontal_branco.png", width=180)
st.markdown("---")

# --- 1) Cards de métricas sempre em linha ---
metrics = [
    ("ABERTOS",       df_ov["TOTAL_CREATED"],   "#222222"),
    ("FINALIZADOS",   df_ov["TOTAL_COMPLETED"], "#222222"),
    ("% PERTINENTES", f"{df_ov['PERCENT_PERTINENTE']:.0f}%", "#222222"),
    ("PENDENTES",     df_ov["TOTAL_ABERTOS"],   "#5A1B1B"),
    ("CLIENTES",      total_clientes,          "#222222"),
    ("INTERNOS",      total_internos,          "#A259FF"),
    ("ATRASADOS",     df_ov["QTD_ATRASADOS"],   "#5A1B1B"),
]

cols = st.columns(len(metrics), gap="small")
for (title, val, bg) , col in zip(metrics, cols):
    col.markdown(
        f"""
        <div style="
            background-color:{bg};
            border-radius:8px;
            padding:1rem;
            text-align:center;
        ">
          <h3 style="margin:0; font-size:2rem; color:#FFFFFF">{val}</h3>
          <p style="margin:0; font-size:0.9rem; color:#DDD; text-transform:uppercase">
            {title}
          </p>
        </div>
        """,
        unsafe_allow_html=True
    )
st.markdown("---")


# --- 2) Layout em duas colunas ---
left, right = st.columns(2, gap="large")

with left:
    # 2.1) Solicitadas há mais tempo
    st.subheader("🕑 Solicitadas há mais tempo")
    df_old = load_data("OLDEST_OPEN_CARDS") \
               .rename(columns={"DIAS_UTEIS_EM_ABERTO":"D.U. Em Aberto"})
    st.dataframe(
        df_old[["ID","D.U. Em Aberto","SOLICITANTE","RESPONSAVEL","STAGE"]],
        use_container_width=True,
        height=300
    )

    st.markdown("")  # espaçamento

    # 2.2) Ranking Solicitantes
    st.subheader("📊 Ranking Solicitantes")
    # agora buscamos a view em snake_case
    df_rank = load_data("TICKETS_BY_SOLICITANTE")[
        ["SOLICITANTE", "CARDS_PENDENTES", "CARDS_DEVOLUTIVA", "TOTAL_CARDS_ABERTOS"]
    ].rename(columns={
        "SOLICITANTE": "Usuário",
        "CARDS_PENDENTES": "Tratamento Interno",   # abertos sem devolutiva
        "CARDS_DEVOLUTIVA": "Devolutiva",
        "TOTAL_CARDS_ABERTOS": "Total"
    })
    st.dataframe(
        df_rank,
        use_container_width=True,
        height=300
    )


with right:

    # --- 2.2) Resumo de Abertos (donut) ---
    st.subheader("🍩 Resumo de Abertos")

    df_donut = load_data("resumo_abertos_donut")

    fig = px.pie(
        df_donut,
        names='responsavel',
        values='qtd',
        hole=0.5,
        color='responsavel',
        color_discrete_map={
            'Comissões': '#F1F1F1',
            'Outras Áreas': '#4F4F4F'
        }
    )

    # empurra um pouco o gráfico para a esquerda e centraliza verticalmente 
    fig.update_traces(
        domain={'x': [0, 0.7], 'y': [0, 1]},            # ocupa 70% da largura disponível
        texttemplate='<b>%{value} (%{percent:.1%})</b>',
        textposition='outside',
        textfont=dict(size=16, color='white'),
        marker=dict(line=dict(color='#121212', width=8))  # borda mais grossa
    )

    fig.update_layout(
        height=300,
        margin=dict(t=20, b=20, l=0, r=0),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        legend=dict(
            orientation='v',
            x=0.85, xanchor='left',
            y=0.5, yanchor='middle',
            font=dict(size=18, color='white')
        )
    )

    st.plotly_chart(fig, use_container_width=True)


    st.markdown("")  # espaçamento

    # 2.4) Solicitadas há mais tempo – Comissões
    st.subheader("🗂️ Solicitadas há mais tempo – Comissões")
    df_comm = df_old[df_old["RESPONSAVEL"] == "Comissões"]
    st.dataframe(
        df_comm[["ID","D.U. Em Aberto","SOLICITANTE","RESPONSAVEL","STAGE"]]
          .reset_index(drop=True),         # zera índice para não aparecer coluna extra
        use_container_width=True,
        height=300
    )
