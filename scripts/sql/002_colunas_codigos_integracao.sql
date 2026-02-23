/*
Adiciona colunas de codigo para suporte de-para:
- CodigoEmpresaContratante
- CodigoSindicato

Aplica em MotoristaCadastro e Afastamento (se as tabelas existirem).
*/

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

IF OBJECT_ID(N'[dbo].[MotoristaCadastro]', N'U') IS NOT NULL
BEGIN
    IF COL_LENGTH(N'dbo.MotoristaCadastro', N'CodigoEmpresaContratante') IS NULL
    BEGIN
        ALTER TABLE [dbo].[MotoristaCadastro]
        ADD [CodigoEmpresaContratante] INT NULL;
    END;

    IF COL_LENGTH(N'dbo.MotoristaCadastro', N'CodigoSindicato') IS NULL
    BEGIN
        ALTER TABLE [dbo].[MotoristaCadastro]
        ADD [CodigoSindicato] INT NULL;
    END;

    UPDATE [dbo].[MotoristaCadastro]
    SET [CodigoEmpresaContratante] = ISNULL([CodigoEmpresaContratante], [NumEmp]),
        [CodigoSindicato] = ISNULL([CodigoSindicato], 273)
    WHERE [CodigoEmpresaContratante] IS NULL
       OR [CodigoSindicato] IS NULL;
END;

IF OBJECT_ID(N'[dbo].[Afastamento]', N'U') IS NOT NULL
BEGIN
    IF COL_LENGTH(N'dbo.Afastamento', N'CodigoEmpresaContratante') IS NULL
    BEGIN
        ALTER TABLE [dbo].[Afastamento]
        ADD [CodigoEmpresaContratante] INT NULL;
    END;

    IF COL_LENGTH(N'dbo.Afastamento', N'CodigoSindicato') IS NULL
    BEGIN
        ALTER TABLE [dbo].[Afastamento]
        ADD [CodigoSindicato] INT NULL;
    END;

    UPDATE [dbo].[Afastamento]
    SET [CodigoEmpresaContratante] = ISNULL([CodigoEmpresaContratante], [NumeroDaEmpresa]),
        [CodigoSindicato] = ISNULL([CodigoSindicato], 273)
    WHERE [CodigoEmpresaContratante] IS NULL
       OR [CodigoSindicato] IS NULL;
END;
