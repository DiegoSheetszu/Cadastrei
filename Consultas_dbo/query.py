from sqlalchemy import text

afastamentos = text("""
    SELECT TOP (:limit)
        a.[numemp],
        a.[tipcol],
        a.[numcad],
        f.[numcpf],
        a.[datafa],
        a.[horafa],
        a.[datter],
        a.[horter],
        a.[prvter],
        a.[sitafa],
        a.[caudem],
        a.[diajus],
        a.[fimqua],
        a.[qhrafa],
        a.[oriafa],
        a.[exmret],
        a.[contov],
        a.[obsafa],
        a.[staatu],
        a.[motrai],
        a.[nroaut],
        a.[codoem],
        a.[codsub],
        a.[datper],
        a.[sitini],
        a.[risnex],
        a.[datnex],
        a.[diaprv],
        a.[codcua],
        a.[tmacua],
        a.[datpar],
        a.[diablq],
        a.[seqreg],
        a.[hrtrcs],
        a.[coddoe],
        a.[codate],
        a.[acitra],
        a.[eferet],
        a.[encafa],
        a.[tipsuc],
        a.[cgcsuc],
        a.[datalt],
        a.[sitori],
        a.[motalt],
        a.[nomate],
        a.[orgcla],
        a.[regcon],
        a.[estcon],
        a.[cgcces],
        a.[onuces],
        a.[cgcsin],
        a.[onusin],
        a.[aciant],
        a.[orimot],
        a.[numpro],
        a.[msmmot],
        a.[cmpau1],
        a.[atepat],
        a.[mancgc],
        a.[manrem],
        a.[codcid],
        a.[indrem],
        a.[cfjsuc],
        a.[cfjces],
        a.[cfjsin],
        a.[cfjmdt]
    FROM [Vetorh_Prod].[Vetorh_Prod].[r038afa] a
    INNER JOIN [Vetorh_Prod].[Vetorh_Prod].[r034fun] f
        ON f.[numemp] = a.[numemp]
        AND f.[tipcol] = a.[tipcol]
        AND f.[numcad] = a.[numcad]
    ORDER BY a.[datafa] DESC, a.[horafa] DESC, a.[seqreg] DESC
""")

cadastro_motoristas = text("""
    WITH FUN AS (
        SELECT f.*
        FROM [Vetorh_Prod].[Vetorh_Prod].[R034FUN] AS f
        WHERE f.SitAfa NOT IN (7)
          AND f.TipCol = 1
          AND f.CodCar = 152292
    ),
    CPL_ESCOLHIDA AS (
        SELECT
            g.*,
            (CASE WHEN g.CodBai IS NOT NULL THEN 1 ELSE 0 END
           + CASE WHEN g.CodCid IS NOT NULL THEN 1 ELSE 0 END
           + CASE WHEN g.CodPai IS NOT NULL THEN 1 ELSE 0 END
           + CASE WHEN g.EndRua IS NOT NULL THEN 1 ELSE 0 END
           + CASE WHEN g.EndNum IS NOT NULL THEN 1 ELSE 0 END
           + CASE WHEN g.DddTel IS NOT NULL THEN 1 ELSE 0 END
           + CASE WHEN g.NumTel IS NOT NULL THEN 1 ELSE 0 END
           + CASE WHEN g.NumCnh IS NOT NULL THEN 1 ELSE 0 END
           + CASE WHEN g.CatCnh IS NOT NULL THEN 1 ELSE 0 END
           + CASE WHEN g.DatCnh IS NOT NULL THEN 1 ELSE 0 END
           + CASE WHEN g.VenCnh IS NOT NULL THEN 1 ELSE 0 END) AS _score,
            ROW_NUMBER() OVER (
                PARTITION BY g.NumCad
                ORDER BY
                    (CASE WHEN g.CodBai IS NOT NULL THEN 1 ELSE 0 END
                   + CASE WHEN g.CodCid IS NOT NULL THEN 1 ELSE 0 END
                   + CASE WHEN g.CodPai IS NOT NULL THEN 1 ELSE 0 END
                   + CASE WHEN g.EndRua IS NOT NULL THEN 1 ELSE 0 END
                   + CASE WHEN g.EndNum IS NOT NULL THEN 1 ELSE 0 END
                   + CASE WHEN g.DddTel IS NOT NULL THEN 1 ELSE 0 END
                   + CASE WHEN g.NumTel IS NOT NULL THEN 1 ELSE 0 END
                   + CASE WHEN g.NumCnh IS NOT NULL THEN 1 ELSE 0 END
                   + CASE WHEN g.CatCnh IS NOT NULL THEN 1 ELSE 0 END
                   + CASE WHEN g.DatCnh IS NOT NULL THEN 1 ELSE 0 END
                   + CASE WHEN g.VenCnh IS NOT NULL THEN 1 ELSE 0 END) DESC,
                    g.NumCad
            ) AS rn
        FROM [Vetorh_Prod].[Vetorh_Prod].[R034CPL] AS g
    )
    SELECT TOP (:limit)
        f.NumCad AS numcad,
        f.NomFun AS nomfun,
        f.NumCpf AS numcpf,
        f.DatNas AS datnas,
        f.TipSex AS tipsex,
        f.DatAdm AS datadm,
        f.DatInc AS datinc,
        f.HorInc AS horinc,
        cid.NomCid AS cidade,
        g.CodEst AS uf,
        g.EndRua AS logradouro,
        bai.NomBai AS bairro,
        g.EndNum AS numero,
        bai.CepBai AS cep
    FROM FUN AS f
    LEFT JOIN CPL_ESCOLHIDA AS g
           ON g.NumCad = f.NumCad
          AND g.rn = 1
    OUTER APPLY (
        SELECT TOP (1) b.NomBai, b.CepBai
        FROM [Vetorh_Prod].[Vetorh_Prod].[R074BAI] AS b
        WHERE b.CodBai = g.CodBai
        ORDER BY b.NomBai
    ) AS bai
    OUTER APPLY (
        SELECT TOP (1) c.NomCid
        FROM [Vetorh_Prod].[Vetorh_Prod].[R074CID] AS c
        WHERE c.CodCid = g.CodCid
        ORDER BY c.NomCid
    ) AS cid
    ORDER BY f.DatInc DESC, f.HorInc DESC, f.DatAdm DESC, f.NumCad DESC
""")
