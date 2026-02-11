from config.engine import ativar_engine
from Consultas_dbo.afastamentos import RepositorioAfastamentos

def main():
    # 1) Escolhe a database de origem
    database = "SOFTRAN_COMTRASIL"

    # 2) Cria engine
    engine = ativar_engine(database)

    # 3) Cria o repositório
    repo = RepositorioAfastamentos(engine)

    # 4) Chama a query
    since = "2026-02-01 00:00:00"
    drivers = repo.buscar_dados_afastamentos(since=since, limit=10)

    # 5) Usa o resultado
    print(f"Total retornado: {len(drivers)}")
    for d in drivers:
        print(d)

if __name__ == "__main__":
    main()



