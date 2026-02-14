import json

from Cadastro_API.login import login_api
from config.engine import ativar_engine
from Consultas_dbo.afastamentos.afastamentos import RepositorioAfastamentos
from Consultas_dbo.cadastro_motoristas.cadastro_motoristas import RepositorioCadastroMotoristas
from Ferramentas.montar_payload_afastamentos import montar_payload_afastamentos
from Ferramentas.montar_payload_motoristas import montar_payload_motoristas


def executar_fluxo_motoristas(engine):
    repo = RepositorioCadastroMotoristas(engine)
    registros = repo.buscar_dados_cadastro_motoristas(limit=1)
    payload = montar_payload_motoristas(registros)

    print(f"[Motoristas] Registros do banco: {len(registros)}")
    print(f"[Motoristas] Registros prontos para API: {len(payload)}")

    if payload:
        print(json.dumps(payload[:1], ensure_ascii=False, indent=2, default=str))
    else:
        print("[Motoristas] Nenhum registro pronto para envio.")


def executar_fluxo_afastamentos(engine):
    repo = RepositorioAfastamentos(engine)
    registros = repo.buscar_dados_afastamentos(limit=1)
    payload = montar_payload_afastamentos(registros)

    print(f"[Afastamentos] Registros do banco: {len(registros)}")
    print(f"[Afastamentos] Registros prontos para API: {len(payload)}")

    if payload:
        print(json.dumps(payload[:1], ensure_ascii=False, indent=2, default=str))
    else:
        print("[Afastamentos] Nenhum registro pronto para envio.")


def main():
    try:
        auth = login_api()
        print(f"Login realizado. Token expira em: {auth.get('exp')}")
    except Exception as exc:
        print(f"Falha no login da API: {exc}")
        return

    engine = ativar_engine("Vetorh_Prod")

    executar_fluxo_motoristas(engine)
    executar_fluxo_afastamentos(engine)


if __name__ == "__main__":
    main()
