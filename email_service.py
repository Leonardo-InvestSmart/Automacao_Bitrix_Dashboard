import msal
import requests
import base64

from config import CLIENT_ID, TENANT_ID, CLIENT_SECRET, EMAIL_USER


def enviar_resumo_email(
    destinatarios: list[str],
    assunto: str,
    corpo: str,
    content_type: str = "Text"
) -> bool:
    """
    Envia um e-mail com assunto e corpo para uma lista de destinatários via Microsoft Graph.

    Parâmetros:
    - destinatarios: lista de endereços de e-mail
    - assunto: assunto do e-mail
    - corpo: conteúdo (plain text ou HTML)
    - content_type: "Text" ou "HTML"
    """
    # Autenticação MSAL
    app = msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}",
        client_credential=CLIENT_SECRET,
    )
    token_resp = app.acquire_token_for_client(
        scopes=["https://graph.microsoft.com/.default"]
    )
    if "access_token" not in token_resp:
        print(f"[EMAIL] Erro ao obter token: {token_resp.get('error_description')}")
        return False

    token = token_resp["access_token"]

    # Payload do e-mail
    mail = {
        "message": {
            "subject": assunto,
            "body": {
                "contentType": content_type,
                "content": corpo,
            },
            "toRecipients": [
                {"emailAddress": {"address": email}} for email in destinatarios
            ],
            "from": {"emailAddress": {"address": EMAIL_USER}},
        },
        "saveToSentItems": "true",
    }

    endpoint = f"https://graph.microsoft.com/v1.0/users/{EMAIL_USER}/sendMail"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    resp = requests.post(endpoint, headers=headers, json=mail)

    if resp.status_code == 202:
        print(f"[EMAIL] Enviado com sucesso para: {destinatarios}")
        return True

    print(f"[EMAIL] Falha ao enviar e-mail: {resp.status_code} – {resp.text}")
    return False


def _get_logo_data_uri() -> str:
    """
    Converte o logo em base64 para inline no HTML.
    Ajuste o caminho se necessário.
    """
    with open("assets/investsmart_horizontal_branco.png", "rb") as f:
        b64 = base64.b64encode(f.read()).decode()
    return f"data:image/png;base64,{b64}"


def _build_email_html(titulo: str, conteudo_html: str) -> str:
    """
    Template padrão de e-mail (header roxo + logo + rodapé).
    Mesmo conceito do SmartC.
    """
    logo_data_uri = _get_logo_data_uri()
    return f"""
    <html>
      <body style="margin:0;padding:0;font-family:Montserrat,sans-serif;background-color:#f4f4f4;">
        <table align="center" width="600" cellpadding="0" cellspacing="0"
               style="background-color:#ffffff;border-radius:8px;overflow:hidden;">
          <!-- header com logo -->
          <tr>
            <td style="background-color:#9966ff;padding:20px;text-align:center;">
              <div style="max-width:300px; margin:0 auto;">
                <img src="{logo_data_uri}" alt="SmartC" width="170" style="display:block;margin:0 auto;" />
              </div>
            </td>
          </tr>
          <!-- título + conteúdo -->
          <tr>
            <td style="padding:20px;">
              <h2 style="color:#4A4A4A;margin-bottom:1rem;">{titulo}</h2>
              {conteudo_html}
            </td>
          </tr>
          <!-- rodapé -->
          <tr>
            <td style="background-color:#f0f0f0;padding:10px;text-align:center;font-size:12px;color:#666;">
              Este é um e-mail automático, por favor não responda.<br/>
              © 2025 InvestSmart – Todos os direitos reservados.
            </td>
          </tr>
        </table>
      </body>
    </html>
    """
