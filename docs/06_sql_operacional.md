# SQL Operacional

## 1. Objetivo

Centralizar consultas SQL para diagnostico, monitoramento e reprocessamento com seguranca.

## 2. Premissas

- Banco alvo: `Cadastrei`
- Schema alvo: `dbo`
- SQL Server: 2014 (evitar uso de funcoes indisponiveis, como `JSON_VALUE`)

## 3. Saude da Fila

## 3.1 Volume por status - Motoristas
```sql
SELECT Status, COUNT(*) AS Total
FROM dbo.MotoristaCadastro
GROUP BY Status
ORDER BY Total DESC;
```

## 3.2 Volume por status - Afastamentos
```sql
SELECT Status, COUNT(*) AS Total
FROM dbo.Afastamento
GROUP BY Status
ORDER BY Total DESC;
```

## 3.3 Erros mais frequentes
```sql
SELECT TOP (20) UltimoErro, COUNT(*) AS Total
FROM dbo.MotoristaCadastro
WHERE Status = 'ERRO'
GROUP BY UltimoErro
ORDER BY Total DESC;
```

## 4. Itens Prontos para Reprocessar

## 4.1 Motoristas em erro
```sql
SELECT TOP (200)
    IdDeOrigem, NumEmp, Status, Tentativas, HttpStatus, UltimoErro, AtualizadoEm
FROM dbo.MotoristaCadastro
WHERE Status = 'ERRO'
ORDER BY AtualizadoEm DESC;
```

## 4.2 Afastamentos em erro
```sql
SELECT TOP (200)
    NumeroDeOrigemDoColaborador, NumEmp, Status, Tentativas, HttpStatus, UltimoErro, AtualizadoEm
FROM dbo.Afastamento
WHERE Status = 'ERRO'
ORDER BY AtualizadoEm DESC;
```

## 5. Reprocessamento (Template)

Use sempre filtro especifico para evitar impacto amplo.

## 5.1 Motoristas (template)
```sql
UPDATE dbo.MotoristaCadastro
SET
    Status = 'PENDENTE',
    Tentativas = 0,
    UltimoErro = NULL,
    HttpStatus = NULL,
    RespostaResumo = NULL,
    LockId = NULL,
    LockEm = NULL,
    ProximaTentativaEm = NULL,
    AtualizadoEm = SYSUTCDATETIME()
WHERE Status = 'ERRO'
  AND IdDeOrigem IN (/* ids */);
```

## 5.2 Afastamentos (template)
```sql
UPDATE dbo.Afastamento
SET
    Status = 'PENDENTE',
    Tentativas = 0,
    UltimoErro = NULL,
    HttpStatus = NULL,
    RespostaResumo = NULL,
    LockId = NULL,
    LockEm = NULL,
    ProximaTentativaEm = NULL,
    AtualizadoEm = SYSUTCDATETIME()
WHERE Status = 'ERRO'
  AND NumeroDeOrigemDoColaborador IN (/* ids */);
```

## 6. Locks

## 6.1 Verificar locks ativos
```sql
SELECT TOP (200)
    Status, LockId, LockEm, AtualizadoEm, Tentativas
FROM dbo.MotoristaCadastro
WHERE LockId IS NOT NULL
ORDER BY LockEm DESC;
```

## 6.2 Liberar locks expirados (manual emergencial)
```sql
UPDATE dbo.MotoristaCadastro
SET LockId = NULL, LockEm = NULL, AtualizadoEm = SYSUTCDATETIME()
WHERE LockId IS NOT NULL
  AND LockEm < DATEADD(MINUTE, -30, SYSUTCDATETIME());
```

## 7. Dicionarios de Apoio

## 7.1 EmpresaDict
```sql
SELECT * FROM dbo.EmpresaDict ORDER BY CodigoEmpresa;
```

## 7.2 SindicatoDict
```sql
SELECT * FROM dbo.SindicatoDict ORDER BY CodigoSindicato;
```

## 8. Checklist de Seguranca SQL

- [ ] Executar update com filtro bem definido.
- [ ] Registrar query aplicada e motivo.
- [ ] Salvar evidencia antes/depois.
- [ ] Evitar alteracoes em horario de pico sem janela.

