import pandas as pd
from config.engine import ativar_engine
from Consultas_dbo.afastamentos.afastamentos import RepositorioAfastamentos


def main():
    database = "SOFTRAN_COMTRASIL"
    engine = ativar_engine(database)

    repo = RepositorioAfastamentos(engine)
    afastamentos = repo.buscar_dados_afastamentos(limit=10)
    df = pd.DataFrame(afastamentos)

    print(f"Total retornado: {len(df)}")
    print(df.head())


if __name__ == "__main__":
    main()
