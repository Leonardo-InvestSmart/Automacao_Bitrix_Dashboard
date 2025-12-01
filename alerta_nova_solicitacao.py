import pytz
import pandas as pd
from datetime import datetime, timedelta

from workalendar.america import BrazilSaoPauloCity
from config import supabase
from email_service import enviar_resumo_email, _build_email_html

# ==============================
# Config
# ==============================
BR_TZ = pytz.timezone("America/Sao_Paulo")
cal = BrazilSaoPauloCity()

DESTINATARIOS = [
    "millena.polizzo@besmart.com.br",
    "marcos.santaclara@investsmart.com.br",
    "orlando.mengali@investsmart.com.br",
]



# ==============================
# Cálculo de horas úteis
# ==============================
def calculate_business_hours(start_dt: datetime, end_dt: datetime) -> float:
    """
    Replica a lógica de cálculo de horas úteis:
    - Considera calendário Brasil / São Paulo (feriados + dias úteis)
    - Janela de 09h às 18h
    """
    if start_dt is None or end_dt is None:
        return 0.0
    if start_dt >= end_dt:
        return 0.0

    # Garantir timezone
    if start_dt.tzinfo is None:
        start_dt = BR_TZ.localize(start_dt)
    if end_dt.tzinfo is None:
        end_dt = BR_TZ.localize(end_dt)

    total_hours = 0.0
    current_dt = start_dt

    while current_dt < end_dt:
        next_dt = min(current_dt + timedelta(hours=1), end_dt)
        current_date = current_dt.date()

        if cal.is_working_day(current_date):
            if 9 <= current_dt.hour < 18:
                fraction = (next_dt - current_dt).total_seconds() / 3600
                total_hours += fraction

        current_dt = next_dt

    return round(float(total_hours), 2)


def parse_historico(historico: str):
    """
    Extrai lista de dicts {status, timestamp} de UF_CRM_335_AUT_HISTORICO.
    Padrão: 'Sistema de Comissões/<status> -&gt; dd/mm/YYYY HH:MM:SS'
    """
    import re

    if not isinstance(historico, str) or not historico.strip():
        return []

    pattern = r"Sistema de Comissões/(.+?)\s*-\&gt;\s*(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2})"
    matches = re.findall(pattern, historico)

    events = []
    for status, ts_str in matches:
        try:
            dt = datetime.strptime(ts_str, "%d/%m/%Y %H:%M:%S")
            dt = BR_TZ.localize(dt)
            events.append({"status": status.strip(), "timestamp": dt})
        except Exception:
            continue
    return events


def horas_uteis_parado_nova_solicitacao(hist_str: str) -> float | None:
    """
    Pega o último momento em que o card entrou em 'Nova Solicitação'
    e calcula horas úteis até agora.
    """
    history = parse_historico(hist_str)
    if not history:
        return None

    # Última ocorrência de "Nova Solicitação"
    ns_events = [ev for ev in history if ev.get("status") == "Nova Solicitação"]
    if not ns_events:
        return None

    # Se houver reentradas, consideramos a última (mais recente)
    t0 = ns_events[-1]["timestamp"]
    now_dt = datetime.now(BR_TZ)

    return calculate_business_hours(t0, now_dt)


# ==============================
# Consulta Supabase
# ==============================
def carregar_cards_nova_solicitacao() -> pd.DataFrame:
    """
    Lê da tabela BITRIX_CARDS apenas os cards em 'Nova Solicitação'.
    Pressupõe que o ETL já upsertou os dados antes via run_etl.py.
    """
    resp = (
        supabase.table("BITRIX_CARDS")
        .select("ID, TITLE, STAGE_NAME, UF_CRM_335_AUT_HISTORICO")
        .eq("STAGE_NAME", "Nova Solicitação")
        .execute()
    )

    data = getattr(resp, "data", None) or resp.get("data", None)
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    return df


def classificar_cards(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enriquece o DataFrame com HORAS_NS e COR (VERMELHO/LARANJA/AMARELO).
    """
    if df.empty:
        return df

    df = df.copy()
    df["HORAS_NS"] = df["UF_CRM_335_AUT_HISTORICO"].apply(horas_uteis_parado_nova_solicitacao)

    def _classify(h):
        if h is None:
            return None
        if h > 8:
            return "VERMELHO"
        if 6 <= h <= 8:
            return "LARANJA"
        if h < 6:
            return "AMARELO"
        return None

    df["COR"] = df["HORAS_NS"].apply(_classify)

    # Só mantém quem conseguiu calcular horas
    df = df[df["HORAS_NS"].notna()].reset_index(drop=True)
    return df


# ==============================
# E-mail
# ==============================
def montar_corpo_email(df: pd.DataFrame) -> str:
    """
    Monta um corpo de e-mail em HTML com:
    - Resumo por cor
    - Tabela dos cards classificados
    """
    if df.empty:
        return """
        <p>Não há cards em <b>Nova Solicitação</b> com horas úteis calculadas no momento.</p>
        """

    resumo = (
        df.groupby("COR")["ID"]
        .count()
        .reindex(["VERMELHO", "LARANJA", "AMARELO"])
        .fillna(0)
        .astype(int)
    )

    red = resumo.get("VERMELHO", 0)
    orange = resumo.get("LARANJA", 0)
    yellow = resumo.get("AMARELO", 0)

    # Pequena tabela
    linhas = []
    for _, row in df.sort_values("HORAS_NS", ascending=False).iterrows():
        linhas.append(
            f"""
            <tr>
              <td>{row.get("ID")}</td>
              <td>{row.get("TITLE", "")}</td>
              <td>{row.get("COR")}</td>
              <td>{row.get("HORAS_NS"):.2f}</td>
            </tr>
            """
        )

    tabela_html = """
    <table border="1" cellspacing="0" cellpadding="4">
      <thead>
        <tr>
          <th>ID</th>
          <th>Título</th>
          <th>Classificação</th>
          <th>Horas úteis em Nova Solicitação</th>
        </tr>
      </thead>
      <tbody>
        {linhas}
      </tbody>
    </table>
    """.format(
        linhas="\n".join(linhas)
    )

    html = f"""
    <p>Segue o status dos cards em <b>Nova Solicitação</b>, considerando horas úteis:</p>
    <ul>
      <li><b>VERMELHO (&gt; 8h úteis):</b> {red}</li>
      <li><b>LARANJA (6h a 8h úteis):</b> {orange}</li>
      <li><b>AMARELO (&lt; 6h úteis):</b> {yellow}</li>
    </ul>
    {tabela_html}
    """

    return html


def enviar_email(df: pd.DataFrame):
    """
    Usa o mesmo fluxo de e-mail do SmartC (Azure + Graph),
    reaproveitando o template corporativo.
    """
    corpo_html_tabela = montar_corpo_email(df)
    agora = datetime.now(BR_TZ).strftime("%d/%m/%Y %H:%M")
    assunto = f"[Financeiro] Cards em 'Nova Solicitação' - {agora}"

    # Embala no template padrão com logo/cores
    html_final = _build_email_html(assunto, corpo_html_tabela)

    ok = enviar_resumo_email(
        destinatarios=DESTINATARIOS,
        assunto=assunto,
        corpo=html_final,
        content_type="HTML",
    )

    if ok:
        print("✅ E-mail enviado com sucesso para:", DESTINATARIOS)
    else:
        print("❌ Falha ao enviar e-mail para:", DESTINATARIOS)



# ==============================
# Main
# ==============================
def main():
    print("▶ Carregando cards em 'Nova Solicitação' do Supabase...")
    df = carregar_cards_nova_solicitacao()
    print(f"Total de cards em Nova Solicitação: {len(df)}")

    df_class = classificar_cards(df)
    print(f"Total de cards com horas úteis calculadas: {len(df_class)}")

    enviar_email(df_class)


if __name__ == "__main__":
    main()
