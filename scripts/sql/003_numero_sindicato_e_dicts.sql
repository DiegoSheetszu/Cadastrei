USE [Cadastrei];
GO

/* 1) Coluna NumeroSindicato nas tabelas de fila */
IF COL_LENGTH('dbo.MotoristaCadastro', 'NumeroSindicato') IS NULL
BEGIN
    ALTER TABLE dbo.MotoristaCadastro ADD NumeroSindicato INT NULL;
END;
GO

IF COL_LENGTH('dbo.Afastamento', 'NumeroSindicato') IS NULL
BEGIN
    ALTER TABLE dbo.Afastamento ADD NumeroSindicato INT NULL;
END;
GO

/* 2) Dicionario de empresas */
IF OBJECT_ID('dbo.EmpresaDict', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.EmpresaDict (
        NumEmp INT NOT NULL PRIMARY KEY,
        Nome NVARCHAR(200) NOT NULL,
        Cnpj VARCHAR(14) NOT NULL,
        Cidade NVARCHAR(100) NOT NULL,
        Uf CHAR(2) NOT NULL,
        Rua NVARCHAR(120) NULL,
        Numero NVARCHAR(20) NULL,
        Complemento NVARCHAR(60) NULL,
        Bairro NVARCHAR(80) NULL,
        Cep VARCHAR(8) NULL,
        Latitude DECIMAL(10, 6) NULL,
        Longitude DECIMAL(10, 6) NULL,
        Ativo BIT NOT NULL CONSTRAINT DF_EmpresaDict_Ativo DEFAULT (1),
        AtualizadoEm DATETIME2(0) NOT NULL CONSTRAINT DF_EmpresaDict_AtualizadoEm DEFAULT SYSUTCDATETIME()
    );
END;
GO

/* 3) Dicionario de sindicatos */
IF OBJECT_ID('dbo.SindicatoDict', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.SindicatoDict (
        NumeroSindicato INT NOT NULL PRIMARY KEY,
        Nome NVARCHAR(200) NOT NULL,
        Cnpj VARCHAR(14) NOT NULL,
        Cidade NVARCHAR(100) NOT NULL,
        Uf CHAR(2) NOT NULL,
        Rua NVARCHAR(120) NULL,
        Numero NVARCHAR(20) NULL,
        Complemento NVARCHAR(60) NULL,
        Bairro NVARCHAR(80) NULL,
        Cep VARCHAR(8) NULL,
        Latitude DECIMAL(10, 6) NULL,
        Longitude DECIMAL(10, 6) NULL,
        Ativo BIT NOT NULL CONSTRAINT DF_SindicatoDict_Ativo DEFAULT (1),
        AtualizadoEm DATETIME2(0) NOT NULL CONSTRAINT DF_SindicatoDict_AtualizadoEm DEFAULT SYSUTCDATETIME()
    );
END;
GO

/* 4) Carga inicial - empresa */
IF EXISTS (SELECT 1 FROM dbo.EmpresaDict WHERE NumEmp = 152)
BEGIN
    UPDATE dbo.EmpresaDict
       SET Nome = N'COMTRASIL COMERCIO E TRANSPORTES LTDA',
           Cnpj = '33899204000165',
           Cidade = N'BRUMADO',
           Uf = 'BA',
           Ativo = 1,
           AtualizadoEm = SYSUTCDATETIME()
     WHERE NumEmp = 152;
END
ELSE
BEGIN
    INSERT INTO dbo.EmpresaDict (NumEmp, Nome, Cnpj, Cidade, Uf, Ativo)
    VALUES (152, N'COMTRASIL COMERCIO E TRANSPORTES LTDA', '33899204000165', N'BRUMADO', 'BA', 1);
END;
GO

/* 5) Carga inicial - sindicato */
IF EXISTS (SELECT 1 FROM dbo.SindicatoDict WHERE NumeroSindicato = 273)
BEGIN
    UPDATE dbo.SindicatoDict
       SET Nome = N'SIND TRAB TRANSP ROD PASSAG CARGAS FRET TURISMO PESSOAL',
           Cnpj = '00063854700096',
           Cidade = N'Vitoria da Conquista',
           Uf = 'BA',
           Ativo = 1,
           AtualizadoEm = SYSUTCDATETIME()
     WHERE NumeroSindicato = 273;
END
ELSE
BEGIN
    INSERT INTO dbo.SindicatoDict (NumeroSindicato, Nome, Cnpj, Cidade, Uf, Ativo)
    VALUES (273, N'SIND TRAB TRANSP ROD PASSAG CARGAS FRET TURISMO PESSOAL', '00063854700096', N'Vitoria da Conquista', 'BA', 1);
END;
GO

/* 6) Backfill padrao do NumeroSindicato */
UPDATE dbo.MotoristaCadastro
   SET NumeroSindicato = 273
 WHERE NumeroSindicato IS NULL;
GO

UPDATE dbo.Afastamento
   SET NumeroSindicato = 273
 WHERE NumeroSindicato IS NULL;
GO
