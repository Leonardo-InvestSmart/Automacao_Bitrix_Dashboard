import pytz
from datetime import datetime

from config import EMAIL_USER
from email_service import enviar_resumo_email

# ==============================
# Config
# ==============================
BR_TZ = pytz.timezone("America/Sao_Paulo")

# âncora (To) para e-mail com BCC em massa
TO_ANCHOR = EMAIL_USER  # ex: comissoes@investsmart.com.br

# ==============================
# BCCs FIXOS
# ==============================

# 1) Comunicado geral
BCC_COMUNICADO = [
    "is.brasil@investsmart.com.br",
    "is.brasil02@investsmart.com.br",
    "is.brasil03@investsmart.com.br",
]

# 2) Gestão / Diretoria
BCC_GESTAO = [
    "adriano.domingues@investsmart.com.br",
    "andre.soares@investsmart.com.br",
    "andrea.cristiana@besmart.com.br",
    "andreia.villasboas@besmart.com.br",
    "ariel.silva@investsmart.com.br",
    "caio.reis@investsmart.com.br",
    "carlos.maxmilian@investsmart.com.br",
    "carlos.neto@investsmart.com.br",
    "catarina.eloy@investsmart.com.br",
    "clovis.alves@besmart.com.br",
    "cristiane.gomes@besmart.com.br",
    "daniel.alves@investsmart.com.br",
    "debora.rosa@investsmart.com.br",
    "fabio.henrique@besmart.com.br",
    "fabio.leal@besmart.com.br",
    "flaviane.felix@besmart.com.br",
    "flavio.lvr@investsmart.com.br",
    "gabriela.soares@investsmart.com.br",
    "jessica.castro@investsmart.com.br",
    "joao.romero@investsmart.com.br",
    "juliana.menezes@besmart.com.br",
    "lara.samenho@investsmart.com.br",
    "leandro.monteiro@investsmart.com.br",
    "leticia.souza@besmart.com.br",
    "luiz.borba@besmart.com.br",
    "mariana.barros@investsmart.com.br",
    "mariana.peres@dolarize.me",
    "matheus.pinho@investsmart.com.br",
    "nathaniel.bessel@investsmart.com.br",
    "polyana.brito@investsmart.com.br",
    "rafael.fischer@investsmart.com.br",
    "renato.carneiro@investsmart.com.br",
    "rennan.rangel@investsmart.com.br",
    "samuel.jose@investsmart.com.br",
    "thomas.oliveira@besmart.com.br",
    "tiago.ninin@investsmart.com.br",
    "vinicius.cabral@investsmart.com.br",
]

# ==============================
# Mensagem - EMAIL (HTML)
# ==============================
MENSAGEM_EMAIL_HTML = """
<strong>Para todos os Assessores, Líderes e Colaboradores da InvestSmart e BeSmart,</strong><br><br>

No final de 2024, a companhia definiu que as comissões BeSmart seriam pagas prioritariamente ao assessor InvestSmart dono do cliente XP. Se não fosse cliente XP, valia o dono da produção no Bitrix.<br><br>

Depois de um ano operando desta forma, evoluímos nos controles e vimos que separar as lógicas torna o processo mais claro, evita conflitos e reduz erros. Por isso, conforme alinhado com a Direção Executiva e CEO, <strong>a partir do fechamento de janeiro de 2026 (pagamento em fevereiro)</strong>, as regras passam a ser:<br><br>

<strong>1. Produções XP Investimentos:</strong><br>
O pagamento seguirá exclusivamente o código A do assessor vinculado à conta do cliente.<br><br>

<strong>2. Produções BeSmart:</strong><br>
O pagamento seguirá exclusivamente o “dono da produção” informado no card do Bitrix.<br><br>

<strong>O que precisa ser ajustado antes do final de janeiro para que o pagamento em fevereiro seja 100% alinhado com o novo formato:</strong><br><br>

• Produções BeSmart <strong>dentro da mesma filial</strong>:<br>
→ O time de comissões enviará um excel para confirmação.<br><br>

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

def main():
    agora = datetime.now(BR_TZ).isoformat()
    print(f"▶ Disparo comunicado iniciado em {agora}")

    if not TO_ANCHOR:
        raise RuntimeError("EMAIL_USER não configurado. Verifique Secrets / variáveis de ambiente.")

    assunto = "[Comunicado Oficial] Regras Comissões 2026 – Fechamento Jan/2026"

    # ==============================
    # Disparo 1 — Comunicado Geral
    # ==============================
    ok_1 = enviar_resumo_email(
        destinatarios=[TO_ANCHOR],
        assunto=assunto,
        corpo=MENSAGEM_EMAIL_HTML,
        content_type="HTML",
        bcc=BCC_COMUNICADO,
        cc=[],
    )

    # ==============================
    # Disparo 2 — Gestão / Diretoria
    # ==============================
    ok_2 = enviar_resumo_email(
        destinatarios=[TO_ANCHOR],
        assunto=assunto,
        corpo=MENSAGEM_EMAIL_HTML,
        content_type="HTML",
        bcc=BCC_GESTAO,
        cc=[],
    )

    if ok_1:
        print("✅ E-mail 1 (Comunicado Geral) enviado com sucesso.")
    else:
        print("❌ Falha no envio do E-mail 1 (Comunicado Geral).")

    if ok_2:
        print("✅ E-mail 2 (Gestão/Diretoria) enviado com sucesso.")
    else:
        print("❌ Falha no envio do E-mail 2 (Gestão/Diretoria).")

    print("🏁 Job concluído.")

if __name__ == "__main__":
    main()
