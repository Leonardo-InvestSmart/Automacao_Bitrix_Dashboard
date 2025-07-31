import streamlit as st
import pandas as pd
import plotly.express as px
from config import supabase
from streamlit_autorefresh import st_autorefresh
import datetime

# --- Configuração da página ---
st.set_page_config(
    page_title="Gestão à Vista – Comissões",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Atualiza a cada 5 minutos (300.000 milissegundos)
st_autorefresh(interval=300000, key="refresh")

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
  margin-bottom: -2rem;      /* elimina a folga abaixo do título */
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

# PENDÊNCIAS: carrega STATUS, QUANTIDADE e CARDS_ATRASADOS
status_df = load_data("VW_STATUS_CARDS")[["STATUS","QUANTIDADE","CARDS_ATRASADOS"]]

# Máscara para “cliente”
mask_cliente = status_df["STATUS"].str.lower().isin(["devolutiva","dados incompletos"])
# Total e atrasos para cliente
pendencia_cliente = int(status_df.loc[mask_cliente, "QUANTIDADE"].sum())
atrasos_cliente   = int(status_df.loc[mask_cliente, "CARDS_ATRASADOS"].sum())
perc_cliente      = (atrasos_cliente / pendencia_cliente * 100) if pendencia_cliente else 0

# Máscara para “comissões” (interno)
mask_com = ~mask_cliente
pendencia_comissoes = int(status_df.loc[mask_com, "QUANTIDADE"].sum())
atrasos_comissoes   = int(status_df.loc[mask_com, "CARDS_ATRASADOS"].sum())
perc_comissoes      = (atrasos_comissoes / pendencia_comissoes * 100) if pendencia_comissoes else 0


# CARDS PENDENTES: soma QUANTIDADE total
total_pendentes = int(
    status_df["QUANTIDADE"].sum()
)

# HEADER
col_title, col_logo = st.columns([4,1])
with col_title:
    st.markdown(
        "<h1 style='color:#FFF; margin:0; padding:0;'>GESTÃO À VISTA – "
        "<span style='color:#A259FF'>COMISSÕES</span></h1>",
        unsafe_allow_html=True
    )

with col_logo:
    st.image(
        "assets/investsmart_horizontal_branco.png",
        width=180,
        use_container_width=False
    )
# st.markdown("---")

# --- 1) Cards de métricas sempre em linha ---
metrics = [
    ("CARDS CRIADOS",       int(df_ov["TOTAL_CREATED"]),       "#222222"),
    ("CARDS FINALIZADOS",   int(df_ov["TOTAL_COMPLETED"]),     "#222222"),
    ("% PERTINENTES",       f"{df_ov['PERCENT_PERTINENTE']:.0f}%", "#222222"),
    ("CARDS PENDENTES",     total_pendentes,       "#5A1B1B"),
    ("PENDÊNCIA CLIENTE (% atrasados)",   f"{pendencia_cliente} ({perc_cliente:.0f}%)", "#222222"),
    ("PENDÊNCIA COMISSÕES (% atrasados)", f"{pendencia_comissoes} ({perc_comissoes:.0f}%)", "#A259FF"),
    ("CARDS ATRASADOS",     int(df_ov["QTD_ATRASADOS"]),       "#5A1B1B"),
]

cols = st.columns(len(metrics), gap="small")
for (title, val, bg), col in zip(metrics, cols):
    col.markdown(
        f"""
        <div style="
            background-color:{bg};
            border-radius:8px;
            padding:1rem;
            display:flex;
            flex-direction:column;
            justify-content:center;
            align-items:center;
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
    st.subheader("Solicitadas há mais tempo")
    df_old = load_data("OLDEST_OPEN_CARDS") \
               .rename(columns={"DIAS_UTEIS_EM_ABERTO":"D.U. Em Aberto", "SOLICITANTE":"Solicitante", "RESPONSAVEL":"Responsável", "STAGE":"Status do Card"}) \

    # — Ajusta case: primeira letra maiúscula, resto minúsculo
    for col in ["Solicitante", "Responsável", "Status do Card"]:
        df_old[col] = df_old[col].str.capitalize()

    # — Cria coluna "Descrição" truncada a 10 palavras
    def truncate_desc(text, max_words=9):
        if not isinstance(text, str):
            return text
        words = text.split()
        return " ".join(words[:max_words]) + "..." if len(words) > max_words else text

    df_old["Descrição"] = df_old["DESCRICAO_PROBLEMA"].apply(truncate_desc)

    st.dataframe(
        df_old[["ID","D.U. Em Aberto","Solicitante","Responsável","Status do Card","Descrição"]],
        use_container_width=True,
        hide_index=True,
        height=740
    )

    st.markdown("")  # espaçamento

with right:

    # --- 2.2) Painel de gráficos à direita em duas colunas ---
    col1, col2 = st.columns(2, gap="large")

    # ----- Coluna 1: Donut + Origem de Comissão -----
    with col1:
        st.subheader("Resumo de Abertos")
        df_donut = load_data("VW_STATUS_CARDS")
        df_donut = df_donut[["RESPONSABILIDADE", "QUANTIDADE"]]

        fig_donut = px.pie(
            df_donut,
            names='RESPONSABILIDADE',
            values='QUANTIDADE',
            hole=0.5,
            color='RESPONSABILIDADE',
            color_discrete_map={
                'Comissões': '#A259FF',
                'Cliente': '#F1F1F1',
                'Externo': '#4F4F4F'
            }
        )
        # Ajusta o domínio para centralizar e coloca a legenda embaixo
        fig_donut.update_traces(
            # move o fundo do donut para cima um pouco (0.1 em vez de 0)
            domain={'x': [0, 1], 'y': [0.1, 0.9]},
            texttemplate='<b>%{value} (%{percent:.1%})</b>',
            textposition='outside',
            textfont=dict(size=18, color='white'),
            marker=dict(line=dict(color='#121212', width=8))
        )
        fig_donut.update_layout(
            height=300,
            margin=dict(t=20, b=80, l=0, r=0),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            legend=dict(
                orientation='h',
                # baixa ainda mais a legenda
                y=-0.3,
                x=0.5,
                xanchor='center',
                font=dict(size=16, color='white')
            )
        )
        st.plotly_chart(fig_donut, use_container_width=True)

        # Pequeno espaçamento
        st.markdown("")


        # Quantidade de Cards por Produto (capitalize e view)
        st.subheader("Quantidade de Cards por Produto")
        df_produto = load_data("VW_PRODUTO_CARDS")
        df_produto.columns = ["Produto", "Quantidade"]
        # formata cada Produto: primeira letra maiúscula, resto minúsculo
        df_produto["Produto"] = df_produto["Produto"].str.capitalize()

        # renomeia "Br global" para "Diversificação"
        df_produto["Produto"] = df_produto["Produto"].replace({
            'Br global': 'Diversificação'
        })

        fig_produto = px.bar(
            df_produto,
            x="Produto",
            y="Quantidade",
            text="Quantidade"
        )
        # cores, ticks e legendas
        fig_produto.update_traces(
            textposition='auto',
            marker_color='white',
            insidetextfont=dict(color='black', size=16),
            outsidetextfont=dict(color='white', size=16)
        )
        fig_produto.update_layout(
            height=400,
            margin=dict(t=20, b=20, l=0, r=0),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            xaxis_title=None,
            yaxis_title=None,
            xaxis=dict(
                tickfont=dict(color='white', size=16),
                showgrid=False
            ),
            yaxis=dict(
                tickfont=dict(color='white', size=16),
                showgrid=False,
                range=[0, df_produto["Quantidade"].max() * 1.05],
                autorange=False
            )
        )
        st.plotly_chart(fig_produto, use_container_width=True)


    # ----- Coluna 2: Funil de Status dos Cards -----
    with col2:
        st.subheader("Distribuição por Status")
        # 1) Carrega apenas STATUS e QUANTIDADE (descarta ORDEM)
        df_status = load_data("VW_STATUS_CARDS")[["STATUS", "QUANTIDADE"]]

        # 2) Renomeia para exibição
        df_status.columns = ["Status", "Quantidade"]

        # 3) Capitaliza corretamente o texto
        df_status["Status"] = df_status["Status"].str.capitalize()

        fig_stage = px.funnel(
            df_status,
            x="Quantidade",
            y="Status"
        )
        # Linha de conexão cinza, texto branco e sempre fora
        fig_stage.update_traces(
            connector=dict(line=dict(color='gray', width=2)),
            textposition='auto',
            insidetextfont=dict(color='black', size=16),
            outsidetextfont=dict(color='white', size=16),
            marker=dict(color='white')
        )
        fig_stage.update_layout(
            height=750,
            margin=dict(t=20, b=20, l=0, r=0),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            yaxis_title=None,
            xaxis_title=None,
            yaxis=dict(
                tickfont=dict(color='white', size=17)
            ),
            xaxis=dict(
                tickfont=dict(color='white', size=14)
            )
        )
        st.plotly_chart(fig_stage, use_container_width=True)

    # ─── nova linha de atualização ───
    st.markdown(
        f"<div style='text-align:right; color:#0f0; font-size:1rem; margin-top:-2rem;'>"
        f"🕒 Última atualização: {datetime.datetime.now():%d/%m/%Y %H:%M:%S}"
        "</div>",
        unsafe_allow_html=True
    )