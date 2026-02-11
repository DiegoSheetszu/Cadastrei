from sqlalchemy import text

afastamentos = text("""
    SELECT TOP (:limit)
        d.tipcol      AS NrMatricula,
        d.numcad              AS NrCadastro,
        d.datafa      AS DataAfastamento,
    FROM dbo.r038afa a
    WHERE a.datafa > :since
    ORDER BY a.datafa DESC
""")