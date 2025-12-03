import pytz
import pandas as pd
from datetime import datetime

from config import supabase
from email_service import enviar_resumo_email

# ==============================
# Config
# ==============================
BR_TZ = pytz.timezone("America/Sao_Paulo")

DESTINATARIOS = [
    "millena.polizzo@besmart.com.br",
    "marcos.santaclara@investsmart.com.br",
    "orlando.mengali@investsmart.com.br",
    # Se quiser, pode manter exatamente o mesmo set do alerta de Nova Solicitação
]

# ==============================
# Consulta Supabase
# ==============================
def carregar_cards_resolucao_nao() -> pd.DataFrame:
    """
    Lê da tabela BITRIX_CARDS apenas os cards com:
      - UF_CRM_335_APROVA_RESOLUCAO = 'NÃO'
      - STAGE_NAME != 'Concluído'
      - STAGE_NAME != 'Cancelado'
    """
    resp = (
        supabase.table("BITRIX_CARDS")
        .select(
            "ID, "
            "STAGE_NAME, "
            "UF_CRM_335_APROVA_RESOLUCAO, "
            "UF_CRM_335_USUARIO_SOLICITANTE, "
            "ASSIGNED_BY_NAME, "
            "CREATED_TIME"
        )
        .eq("UF_CRM_335_APROVA_RESOLUCAO", "NÃO")
        .eq("STAGE_NAME", "Retorno Devolutiva")
        .execute()
    )

    data = getattr(resp, "data", None) or resp.get("data", None)
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    return df


# ==============================
# E-mail
# ==============================
def montar_corpo_email(df: pd.DataFrame) -> str:
    """
    Monta corpo do e-mail em HTML com:
    - Resumo por estágio
    - Tabela de cards pendentes de resolução (APROVA_RESOLUCAO = "NÃO")
    """
    if df.empty:
        return """
        <p>Não há cards com <b>UF_CRM_335_APROVA_RESOLUCAO = "NÃO"</b> em aberto
        (não concluídos/cancelados) no momento.</p>
        """

    # Resumo por responsável
    resumo_responsavel = (
        df.groupby("ASSIGNED_BY_NAME")["ID"]
        .count()
        .sort_values(ascending=False)
    )

    linhas_resumo = []
    for responsavel, qtd in resumo_responsavel.items():
        nome_resp = responsavel or "Sem responsável"
        linhas_resumo.append(f"<li><b>{nome_resp}:</b> {qtd} card(s)</li>")

    resumo_html = "<ul>" + "\n".join(linhas_resumo) + "</ul>"

    # Tabela detalhada
    linhas = []
    for _, row in df.sort_values("ASSIGNED_BY_NAME").iterrows():
        solicitante = row.get("UF_CRM_335_USUARIO_SOLICITANTE") or ""
        stage = row.get("STAGE_NAME") or ""
        responsavel = row.get("ASSIGNED_BY_NAME") or ""
        created_time_raw = row.get("CREATED_TIME") or ""

        # Formata "2025-11-12T10:01:15" -> "12/11/2025 às 10:01"
        created_time_fmt = created_time_raw
        if created_time_raw:
            try:
                dt = datetime.fromisoformat(str(created_time_raw).replace("Z", ""))
                created_time_fmt = dt.strftime("%d/%m/%Y às %H:%M")
            except Exception:
                # Se der erro na conversão, deixa o valor original
                created_time_fmt = created_time_raw

        linhas.append(
            f"""
            <tr>
              <td>{row.get("ID")}</td>
              <td>{stage}</td>
              <td>{solicitante}</td>
              <td>{responsavel}</td>
              <td>{created_time_fmt}</td>
            </tr>
            """
        )



    tabela_html = """
    <table border="1" cellspacing="0" cellpadding="4">
      <thead>
        <tr>
          <th>ID</th>
          <th>Estágio Atual</th>
          <th>Solicitante</th>
          <th>Responsável</th>
          <th>Data de Criação</th>
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
    <p>Segue o status dos cards com
       <b>UF_CRM_335_APROVA_RESOLUCAO = "NÃO"</b>
       e estágio igual a <b>Retorno Devolutiva</b>:</p>

    <p><b>Resumo por responsável:</b></p>
    {resumo_html}

    <p><b>Detalhamento dos cards:</b></p>
    {tabela_html}
    """

    return html


def enviar_email(df: pd.DataFrame):
    """
    Envia o e-mail com o HTML simples da tabela, sem template adicional.
    """
    corpo_html_tabela = montar_corpo_email(df)
    agora = datetime.now(BR_TZ).strftime("%d/%m/%Y %H:%M")
    assunto = f"[Financeiro] Cards com resolução pendente (APROVA_RESOLUCAO = NÃO) - {agora}"

    ok = enviar_resumo_email(
        destinatarios=DESTINATARIOS,
        assunto=assunto,
        corpo=corpo_html_tabela,
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
    print("▶ Carregando cards com APROVA_RESOLUCAO = 'NÃO' e não concluídos/cancelados...")
    df = carregar_cards_resolucao_nao()
    print(f"Total de cards encontrados: {len(df)}")

    enviar_email(df)


if __name__ == "__main__":
    main()
