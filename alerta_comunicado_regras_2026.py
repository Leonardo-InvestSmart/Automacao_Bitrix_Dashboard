import re
import time
import calendar
import pandas as pd
import pytz
from datetime import datetime, date

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
# Mensagem - EMAIL (HTML)
# ==============================
MENSAGEM_EMAIL_HTML = """
<strong>Para todos os Assessores, Líderes e Colaboradores da InvestSmart e BeSmart,</strong><br><br>

No final de 2024, a companhia definiu que as comissões BeSmart seriam pagas prioritariamente ao assessor InvestSmart dono do cliente XP. Se não fosse cliente XP, valia o dono da produção no Bitrix.<br><br>

Depois de um ano operando assim, evoluímos nos controles e vimos que separar as lógicas torna o processo mais claro, evita conflitos e reduz erros. Por isso, conforme alinhado com a Direção Executiva e CEO, <strong>a partir do fechamento de janeiro de 2026 (pagamento em fevereiro)</strong>, as regras passam a ser:<br><br>

<strong>1. Produções XP Investimentos:</strong><br>
O pagamento seguirá exclusivamente o código A do assessor vinculado à conta do cliente.<br><br>

<strong>2. Produções BeSmart:</strong><br>
O pagamento seguirá exclusivamente o “dono da produção” informado no card do Bitrix.<br><br>

<strong>O que precisa ser ajustado antes do final de janeiro para que o pagamento em fevereiro seja 100% alinhado com o novo formato:</strong><br><br>

• Produções BeSmart <strong>dentro da mesma filial</strong>:<br>
→ O time de comissões enviará aos líderes um excel para confirmação.<br><br>

• Produções BeSmart <strong>entre filiais diferentes</strong>:<br>
→ Diretores receberão um excel para validação junto aos líderes e assessores.<br><br>

• Cards onde <strong>capitães</strong> aparecem como donos:<br>
→ Líderes indicarão o verdadeiro dono da produção.<br><br>

Independente dos casos acima:<br>
• <strong>Conferir base XP</strong> para garantir que os códigos A estejam corretos.<br>
• <strong>Garantir o dono correto no card desde o início em 2026.</strong><br><br>

<strong>Por que estamos mudando</strong><br><br>

A separação das regras deixa o processo mais simples, transparente e justo. Também acompanha a realidade atual: muitos assessores têm se especializado na BeSmart como uma das principais fontes de receita.<br><br>

Vamos para cima em 2026 com muito mais praticidade e simplicidade nas regras, focar no que importa que é vender com a tranquilidade que irá receber.<br><br>

Atenciosamente,<br><br>
<strong>Equipe de Comissões</strong>
""".strip()

# ==============================
# Mensagem - WHATSAPP
# ==============================
MENSAGEM_WHATSAPP = """
*Para todos os Assessores, Líderes e Colaboradores da InvestSmart e BeSmart,*

No final de 2024, a companhia definiu que as comissões BeSmart seriam pagas prioritariamente ao assessor InvestSmart dono do cliente XP. Se não fosse cliente XP, valia o dono da produção no Bitrix.

Depois de um ano operando assim, evoluímos nos controles e vimos que separar as lógicas torna o processo mais claro, evita conflitos e reduz erros. Por isso, conforme alinhado com a Direção Executiva e CEO, *a partir do fechamento de janeiro de 2026 (pagamento em fevereiro)*, as regras passam a ser:

*1. Produções XP Investimentos*  
Pagamento exclusivo pelo código A do assessor vinculado à conta do cliente.

*2. Produções BeSmart*  
Pagamento exclusivo pelo dono da produção informado no card do Bitrix.

*O que precisa ser ajustado antes do final de janeiro:*

• BeSmart na mesma filial (lógica 2025)  
→ Excel será enviado aos líderes para confirmação.

• BeSmart entre filiais  
→ Diretores validarão junto aos líderes e assessores.

• Cards com capitães como donos  
→ Líderes indicarão o verdadeiro dono.

*Para todos os líderes e assessores:*
• Conferir base XP (código A correto).  
• Garantir dono correto no card desde o início em 2026.

*Por que estamos mudando*

Processo mais simples, transparente e justo, refletindo a realidade atual da BeSmart.

Vamos para cima em 2026 com mais praticidade e segurança no recebimento.

Atenciosamente,

*Equipe de Comissões*
""".strip()


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
    # ==============================
    # Travas de execução (governança)
    # - Dias úteis
    # - Somente até o último dia do mês corrente (dia 31 quando existir)
    # ==============================
    hoje = datetime.now(BR_TZ).date()

    # Dias úteis: seg(0) ... sex(4)
    if hoje.weekday() >= 5:
        print("⏹️ Fim de semana. Job abortado.")
        return

    # Até o último dia do mês corrente
    ultimo_dia_mes = calendar.monthrange(hoje.year, hoje.month)[1]
    data_limite = date(hoje.year, hoje.month, ultimo_dia_mes)

    if hoje > data_limite:
        print("⏹️ Fora do período permitido (após o fim do mês). Job abortado.")
        return

    # Garantir âncora de e-mail
    if not TO_ANCHOR:
        raise RuntimeError("EMAIL_USER não configurado. Verifique GitHub Secrets/env.")

    assunto = "[Comunicado Oficial] Regras Comissões 2026 – Fechamento Jan/2026"

    print("▶ Carregando contatos:", BASE_EMAILS_PATH)
    emails, telefones = carregar_contatos(BASE_EMAILS_PATH)
    print(f"📧 Emails (BCC): {len(emails)} | 💬 Telefones: {len(telefones)}")

    # ========== E-MAIL via Azure (Microsoft Graph) ==========
    # Estratégia: 1 e-mail com TO âncora e todos os destinatários em BCC
    ok = enviar_resumo_email(
        destinatarios=[TO_ANCHOR],
        assunto=assunto,
        corpo=MENSAGEM_EMAIL_HTML,
        content_type="HTML",
        bcc=emails,
    )

    if ok:
        print("✅ E-mail Azure enviado com sucesso (BCC).")
    else:
        print("❌ Falha ao enviar e-mail Azure (BCC).")

    # ========== WhatsApp via Z-API ==========
    enviar_whatsapp_zapi(MENSAGEM_WHATSAPP, telefones)

    print("🏁 Job concluído.")


if __name__ == "__main__":
    main()
