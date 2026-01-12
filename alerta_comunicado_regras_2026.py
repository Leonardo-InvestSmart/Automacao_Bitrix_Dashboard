import re
import time
import pandas as pd
import pytz
from datetime import datetime

from config import EMAIL_USER, ZAPI_INSTANCE_ID, ZAPI_INSTANCE_TOKEN, ZAPI_ACCOUNT_SECURITY_TOKEN
from email_service import enviar_resumo_email


# ==============================
# Config
# ==============================
BR_TZ = pytz.timezone("America/Sao_Paulo")
BASE_EMAILS_PATH = "assets/base_emails.xlsx"

# âncora (To) para e-mail com BCC em massa
TO_ANCHOR = EMAIL_USER  # ex: comissoes@investsmart.com.br


# ==============================
# Mensagem (mesma para e-mail e WhatsApp)
# ==============================
MENSAGEM_TEXTO = """
Para todos os Assessores, Líderes e Colaboradores da InvestSmart e BeSmart,
No final de 2024, a companhia definiu que as comissões BeSmart seriam pagas prioritariamente ao assessor InvestSmart dono do cliente XP. Se não fosse cliente XP, valia o dono da produção no Bitrix.
Depois de um ano operando assim, evoluímos nos controles e vimos que separar as lógicas torna o processo mais claro, evita conflitos e reduz erros. Por isso, conforme alinhado com a Direção Executiva e CEO, a partir do fechamento de janeiro de 2026 (pagamento em fevereiro), as regras passam a ser:
1. Produções XP Investimentos: O pagamento seguirá exclusivamente o código A do assessor vinculado a conta do cliente.
 2. Produções BeSmart: O pagamento seguirá exclusivamente o “dono da produção” informado no card do Bitrix.
O que precisa ser ajustado antes do final de janeiro para que o pagamento em fevereiro seja 100% alinhado com o novo formato:
• Produções BeSmart dentro da mesma filial que estejam sendo redirecionadas na lógica de 2025:
→ O time de comissões enviará aos líderes um excel para confirmar o dono correto de cada produção. Esse excel deverá ser retornado até um prazo a ser definido.
• Produções BeSmart que estejam sendo redirecionadas entre filiais diferentes:
→ Diretores receberão um excel do time de comissões onde validarão o dono correto junto dos líderes e assessores. Esse excel deverá ser retornado até um prazo a ser definido.
• Cards onde capitães aparecem como donos:
→ Líderes receberão do time de comissões um excel onde indicarão quem é o verdadeiro dono da produção. Esse excel deverá ser retornado até um prazo a ser definido.
Independente dos casos acima, vale para todos líderes e assessores nessa transição:
• Conferir base XP para garantir que os códigos A estejam corretos.
• A partir de 2026, garantir que o dono correto esteja definido no card desde o início junto do comercial BeSmart ou capitão.
Por que estamos mudando
A separação das regras deixa o processo mais simples, transparente e justo. Também acompanha a realidade atual: muitos assessores têm se especializado na BeSmart como uma das principais fontes de receita.
Vamos para cima em 2026 com muito mais praticidade e simplicidade nas regras, focar no que importa que é vender com a tranquilidade que irá receber.
""".strip()


def texto_para_html(texto: str) -> str:
    safe = (
        texto.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;")
    )
    return (
        "<div style='font-family: Arial, sans-serif; font-size: 14px; line-height: 1.5;'>"
        + safe.replace("\n", "<br>")
        + "</div>"
    )


def normalizar_telefone_br(phone: str) -> str | None:
    digits = re.sub(r"\D", "", str(phone or ""))
    if not digits:
        return None
    if not digits.startswith("55"):
        digits = "55" + digits
    # mínimo: 55 + DDD + 8/9 dígitos
    if len(digits) < 12:
        return None
    return digits


def carregar_contatos(path: str) -> tuple[list[str], list[str]]:
    df = pd.read_excel(path, engine="openpyxl")
    df.columns = [str(c).strip().upper() for c in df.columns]

    if "EMAIL" not in df.columns:
        raise ValueError('Planilha precisa ter a coluna "EMAIL".')

    emails = (
        df["EMAIL"].dropna().astype(str).str.strip()
          .loc[lambda s: s.str.contains("@", na=False)]
          .unique().tolist()
    )

    telefones = []
    if "TELEFONE" in df.columns:
        for t in df["TELEFONE"].dropna().tolist():
            n = normalizar_telefone_br(t)
            if n:
                telefones.append(n)
        # dedup preservando ordem
        telefones = list(dict.fromkeys(telefones))

    return emails, telefones


def enviar_whatsapp_zapi(message: str, phones: list[str]) -> None:
    import requests

    if not phones:
        print("⚠️ Nenhum telefone válido para WhatsApp.")
        return

    if not (ZAPI_INSTANCE_ID and ZAPI_INSTANCE_TOKEN and ZAPI_ACCOUNT_SECURITY_TOKEN):
        raise RuntimeError("Z-API não configurado (INSTANCE_ID/INSTANCE_TOKEN/ACCOUNT_SECURITY_TOKEN).")

    url = f"https://api.z-api.io/instances/{ZAPI_INSTANCE_ID}/token/{ZAPI_INSTANCE_TOKEN}/send-text"
    headers = {
        "Client-Token": ZAPI_ACCOUNT_SECURITY_TOKEN,
        "Content-Type": "application/json",
    }

    ok, fail = 0, 0
    for i, phone in enumerate(phones, start=1):
        try:
            resp = requests.post(url, headers=headers, json={"phone": phone, "message": message}, timeout=30)
            resp.raise_for_status()
            ok += 1
            print(f"✅ ({i}/{len(phones)}) WhatsApp enviado → {phone}")
        except Exception as e:
            fail += 1
            print(f"❌ ({i}/{len(phones)}) Erro WhatsApp → {phone}: {e}")
        time.sleep(1.2)

    print(f"📌 WhatsApp finalizado | OK: {ok} | Falhas: {fail}")


def main():
    agora = datetime.now(BR_TZ).strftime("%d/%m/%Y %H:%M")
    assunto = f"[Comunicado Oficial] Regras Comissões 2026 – Fechamento Jan/2026 | {agora}"

    print("▶ Carregando contatos:", BASE_EMAILS_PATH)
    emails, telefones = carregar_contatos(BASE_EMAILS_PATH)
    print(f"📧 Emails (BCC): {len(emails)} | 💬 Telefones: {len(telefones)}")

    # ========== E-MAIL via Azure (Microsoft Graph) ==========
    # Estratégia: 1 e-mail com TO âncora e todos os destinatários em BCC
    corpo_html = texto_para_html(MENSAGEM_TEXTO)

    ok = enviar_resumo_email(
        destinatarios=[TO_ANCHOR],     # To
        assunto=assunto,
        corpo=corpo_html,
        content_type="HTML",
        bcc=emails,                   # <-- precisamos suportar isso no email_service.py
    )

    if ok:
        print("✅ E-mail Azure enviado com sucesso (BCC).")
    else:
        print("❌ Falha ao enviar e-mail Azure (BCC).")

    # ========== WhatsApp via Z-API ==========
    enviar_whatsapp_zapi(MENSAGEM_TEXTO, telefones)

    print("🏁 Job concluído.")


if __name__ == "__main__":
    main()
