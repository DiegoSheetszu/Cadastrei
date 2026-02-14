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
    SELECT TOP (:limit)
        [numemp],
        [tipcol],
        [numcad],
        [nomfun],
        [datnas],
        [tipsex],
        [numcpf],
        [numcra],
        [catter],
        [datadm],
        [codcar],
        [codccu],
        [codfil],
        [seqreg]
    FROM [Vetorh_Prod].[Vetorh_Prod].[r034fun]
    ORDER BY [datinc] DESC, [horinc] DESC, [seqreg] DESC
""")
