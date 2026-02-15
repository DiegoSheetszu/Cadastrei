import re

from sqlalchemy import text

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quoted_identifier(name: str, label: str) -> str:
    value = (name or "").strip()
    if not _IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"{label} invalido: {name!r}")
    return f"[{value}]"


def _table(schema: str, table_name: str) -> str:
    return f"{_quoted_identifier(schema, 'Schema')}.{_quoted_identifier(table_name, 'Tabela')}"


def montar_query_afastamentos(schema_origem: str):
    r038afa = _table(schema_origem, "R038AFA")
    r034fun = _table(schema_origem, "R034FUN")

    return text(f"""
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
        FROM {r038afa} AS a
        INNER JOIN {r034fun} AS f
            ON f.[numemp] = a.[numemp]
            AND f.[tipcol] = a.[tipcol]
            AND f.[numcad] = a.[numcad]
        ORDER BY a.[datafa] DESC, a.[horafa] DESC, a.[seqreg] DESC
    """)


def _sql_cadastro_motoristas(schema_origem: str, top_clause: str, where_clause: str = "") -> str:
    r034fun = _table(schema_origem, "R034FUN")
    r034cpl = _table(schema_origem, "R034CPL")
    r074bai = _table(schema_origem, "R074BAI")
    r074cid = _table(schema_origem, "R074CID")
    r074pai = _table(schema_origem, "R074PAI")

    where_extra = f"\n        WHERE {where_clause}" if where_clause else ""

    return f"""
        WITH FUN AS (
            SELECT f.*
            FROM {r034fun} AS f
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
            FROM {r034cpl} AS g
        )
        SELECT {top_clause}
            f.NumEmp AS numemp,
            f.TipCol AS tipcol,
            f.NumCad AS numcad,
            f.NomFun AS nomfun,
            f.NumCpf AS numcpf,
            f.DatNas AS datnas,
            f.TipSex AS tipsex,
            f.DatAdm AS datadm,
            f.SitAfa AS sitafa,
            f.CodCcu AS codccu,
            f.DatInc AS datinc,
            f.HorInc AS horinc,
            cid.NomCid AS cidade,
            g.CodEst AS uf,
            g.CodEst AS estado_residencia,
            g.CodPai AS codpai,
            pai.NomPai AS pais,
            TRY_CONVERT(INT, g.NumCid) AS naturalidade,
            g.EndRua AS logradouro,
            bai.NomBai AS bairro,
            g.EndNum AS numero,
            bai.CepBai AS cep,
            g.DocIdn AS numero_rg,
            g.EmiCid AS orgao_expedidor_rg,
            g.NumCnh AS numcnh,
            g.CatCnh AS catcnh,
            g.DatCnh AS datcnh,
            g.VenCnh AS vencnh,
            g.PriCnh AS pricnh,
            g.DddTel AS dddtel,
            g.NumTel AS numtel,
            f.EstCiv AS estado_civil,
            CAST(NULL AS NVARCHAR(200)) AS nome_mae
        FROM FUN AS f
        LEFT JOIN CPL_ESCOLHIDA AS g
            ON g.NumCad = f.NumCad
            AND g.rn = 1
        OUTER APPLY (
            SELECT TOP (1) b.NomBai, b.CepBai
            FROM {r074bai} AS b
            WHERE b.CodBai = g.CodBai
            ORDER BY b.NomBai
        ) AS bai
        OUTER APPLY (
            SELECT TOP (1) c.NomCid
            FROM {r074cid} AS c
            WHERE c.CodCid = g.CodCid
            ORDER BY c.NomCid
        ) AS cid
        OUTER APPLY (
            SELECT TOP (1) p.NomPai
            FROM {r074pai} AS p
            WHERE p.CodPai = g.CodPai
            ORDER BY p.NomPai
        ) AS pai
        {where_extra}
        ORDER BY f.DatInc DESC, f.HorInc DESC, f.DatAdm DESC, f.NumCad DESC
    """


def montar_query_cadastro_motoristas(schema_origem: str):
    return text(_sql_cadastro_motoristas(schema_origem, top_clause="TOP (:limit)"))


def montar_query_cadastro_motoristas_por_numcads(schema_origem: str, placeholders: str):
    if not placeholders.strip():
        raise ValueError("Placeholders de NumCad nao informados")
    return text(
        _sql_cadastro_motoristas(
            schema_origem,
            top_clause="",
            where_clause=f"f.NumCad IN ({placeholders})",
        )
    )


afastamentos = montar_query_afastamentos("Vetorh_Prod")
cadastro_motoristas = montar_query_cadastro_motoristas("Vetorh_Prod")
