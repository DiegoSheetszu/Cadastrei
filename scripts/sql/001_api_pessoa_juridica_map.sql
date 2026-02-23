/*
Tabela de mapeamento de Pessoa Juridica por codigo da empresa (NumEmp),
com suporte a escopo por cliente e ambiente.

Usada pelo dispatcher da API para preencher campos obrigatorios de:
- empregador
- sindicato
*/

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

IF OBJECT_ID(N'[dbo].[ApiPessoaJuridicaMap]', N'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[ApiPessoaJuridicaMap](
        [Id] UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT [PK_ApiPessoaJuridicaMap] PRIMARY KEY
            CONSTRAINT [DF_ApiPessoaJuridicaMap_Id] DEFAULT NEWID(),
        [CodigoEmpresa] INT NOT NULL,
        [CodigoPessoa] INT NULL, -- codigo enviado no payload (ex.: empregador.codigo / sindicato.codigo)
        [TipoPessoa] VARCHAR(20) NOT NULL, -- EMPREGADOR | SINDICATO
        [Nome] NVARCHAR(200) NOT NULL,
        [Cnpj] VARCHAR(14) NOT NULL, -- somente digitos
        [Rua] NVARCHAR(120) NULL,
        [Numero] NVARCHAR(20) NULL,
        [Complemento] NVARCHAR(60) NULL,
        [Bairro] NVARCHAR(80) NULL,
        [Cidade] NVARCHAR(80) NOT NULL,
        [Uf] CHAR(2) NOT NULL,
        [Cep] VARCHAR(8) NULL, -- somente digitos
        [Latitude] DECIMAL(10, 6) NULL,
        [Longitude] DECIMAL(10, 6) NULL,
        [ClienteApiId] UNIQUEIDENTIFIER NULL, -- opcional (clientes_api.json -> id)
        [Ambiente] VARCHAR(20) NULL,          -- opcional: HOM, PROD...
        [Prioridade] INT NOT NULL
            CONSTRAINT [DF_ApiPessoaJuridicaMap_Prioridade] DEFAULT (0),
        [Ativo] BIT NOT NULL
            CONSTRAINT [DF_ApiPessoaJuridicaMap_Ativo] DEFAULT (1),
        [CriadoEm] DATETIME2(0) NOT NULL
            CONSTRAINT [DF_ApiPessoaJuridicaMap_CriadoEm] DEFAULT SYSUTCDATETIME(),
        [AtualizadoEm] DATETIME2(0) NOT NULL
            CONSTRAINT [DF_ApiPessoaJuridicaMap_AtualizadoEm] DEFAULT SYSUTCDATETIME()
    );
END;

IF OBJECT_ID(N'[dbo].[ApiPessoaJuridicaMap]', N'U') IS NOT NULL
AND COL_LENGTH(N'dbo.ApiPessoaJuridicaMap', N'CodigoPessoa') IS NULL
BEGIN
    ALTER TABLE [dbo].[ApiPessoaJuridicaMap]
    ADD [CodigoPessoa] INT NULL;
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.check_constraints
    WHERE [name] = N'CK_ApiPessoaJuridicaMap_TipoPessoa'
)
BEGIN
    ALTER TABLE [dbo].[ApiPessoaJuridicaMap]
    ADD CONSTRAINT [CK_ApiPessoaJuridicaMap_TipoPessoa]
    CHECK (UPPER(LTRIM(RTRIM([TipoPessoa]))) IN ('EMPREGADOR', 'SINDICATO'));
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_ApiPessoaJuridicaMap_Lookup'
      AND [object_id] = OBJECT_ID(N'[dbo].[ApiPessoaJuridicaMap]')
)
BEGIN
    CREATE INDEX [IX_ApiPessoaJuridicaMap_Lookup]
    ON [dbo].[ApiPessoaJuridicaMap](
        [CodigoEmpresa],
        [TipoPessoa],
        [ClienteApiId],
        [Ambiente],
        [Ativo],
        [Prioridade]
    );
END;

/* Registro padrao do empregador informado */
MERGE [dbo].[ApiPessoaJuridicaMap] AS target
USING (
    SELECT
        CAST(152 AS INT) AS [CodigoEmpresa],
        CAST(152 AS INT) AS [CodigoPessoa],
        CAST('EMPREGADOR' AS VARCHAR(20)) AS [TipoPessoa],
        CAST('COMTRASIL COMERCIO E TRANSPORTES LTDA' AS NVARCHAR(200)) AS [Nome],
        CAST('33899204000165' AS VARCHAR(14)) AS [Cnpj],
        CAST('BRUMADO' AS NVARCHAR(80)) AS [Cidade],
        CAST('BA' AS CHAR(2)) AS [Uf]
) AS src
ON  target.[CodigoEmpresa] = src.[CodigoEmpresa]
AND UPPER(LTRIM(RTRIM(target.[TipoPessoa]))) = src.[TipoPessoa]
AND target.[ClienteApiId] IS NULL
AND target.[Ambiente] IS NULL
WHEN MATCHED THEN
    UPDATE SET
        target.[Nome] = src.[Nome],
        target.[Cnpj] = src.[Cnpj],
        target.[CodigoPessoa] = src.[CodigoPessoa],
        target.[Cidade] = src.[Cidade],
        target.[Uf] = src.[Uf],
        target.[Ativo] = 1,
        target.[AtualizadoEm] = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (
        [CodigoEmpresa],
        [CodigoPessoa],
        [TipoPessoa],
        [Nome],
        [Cnpj],
        [Cidade],
        [Uf],
        [Ativo],
        [Prioridade]
    )
    VALUES (
        src.[CodigoEmpresa],
        src.[CodigoPessoa],
        src.[TipoPessoa],
        src.[Nome],
        src.[Cnpj],
        src.[Cidade],
        src.[Uf],
        1,
        0
    );

MERGE [dbo].[ApiPessoaJuridicaMap] AS target
USING (
    SELECT
        CAST(152 AS INT) AS [CodigoEmpresa],
        CAST(273 AS INT) AS [CodigoPessoa],
        CAST('SINDICATO' AS VARCHAR(20)) AS [TipoPessoa],
        CAST('SIND TRAB TRANSP ROD PASSAG CARGAS FRET TURISMO PESSOAL' AS NVARCHAR(200)) AS [Nome],
        CAST('00063854700096' AS VARCHAR(14)) AS [Cnpj],
        CAST('VITORIA DA CONQUISTA' AS NVARCHAR(80)) AS [Cidade],
        CAST('BA' AS CHAR(2)) AS [Uf]
) AS src
ON  target.[CodigoEmpresa] = src.[CodigoEmpresa]
AND UPPER(LTRIM(RTRIM(target.[TipoPessoa]))) = src.[TipoPessoa]
AND target.[ClienteApiId] IS NULL
AND target.[Ambiente] IS NULL
WHEN MATCHED THEN
    UPDATE SET
        target.[Nome] = src.[Nome],
        target.[Cnpj] = src.[Cnpj],
        target.[CodigoPessoa] = src.[CodigoPessoa],
        target.[Cidade] = src.[Cidade],
        target.[Uf] = src.[Uf],
        target.[Ativo] = 1,
        target.[AtualizadoEm] = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (
        [CodigoEmpresa], [CodigoPessoa], [TipoPessoa], [Nome], [Cnpj], [Cidade], [Uf], [Ativo], [Prioridade]
    )
    VALUES (
        src.[CodigoEmpresa], src.[CodigoPessoa], src.[TipoPessoa], src.[Nome], src.[Cnpj], src.[Cidade], src.[Uf], 1, 0
    );
