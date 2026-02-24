# Setup, Build e Deploy

## 1. Objetivo

Padronizar setup local, build de executaveis e deploy para servidor Windows com minimo risco operacional.

## 2. Pre-requisitos

## 2.1 Servidor de Aplicacao
- [ ] Acesso ao SQL Server remoto liberado.
- [ ] Porta SQL validada (ex.: `1433`).
- [ ] Driver ODBC instalado e validado.
- [ ] Permissao para criar/iniciar servicos Windows.
- [ ] Pasta `C:\\Cadastrei` criada.

## 2.2 Estacao de Build
- [ ] Python e venv configurados.
- [ ] Dependencias instaladas.
- [ ] PyInstaller funcional.
- [ ] Repositorio atualizado.

## 3. Estrutura de Pastas Alvo

```text
C:\Cadastrei\
  .env
  clientes_api.json
  apps\
    prod\
    hom\
    ui\
  deploy\
  logs\
  build\
```

## 4. Build de Executaveis

## 4.1 Build completo por ambiente
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\build_executaveis.ps1 -DestinoRaiz C:\Cadastrei -Ambiente Producao
powershell -ExecutionPolicy Bypass -File .\scripts\build_executaveis.ps1 -DestinoRaiz C:\Cadastrei -Ambiente Homologacao
```

## 4.2 Build alvo unico (exemplo API afastamentos prod)
Usar PyInstaller direto para build parcial quando necessario.

## 5. Deploy por Copia

- [ ] Copiar pasta do executavel para `C:\\Cadastrei\\apps\\...`.
- [ ] Copiar `.env` para `C:\\Cadastrei\\.env`.
- [ ] Copiar `clientes_api.json` para `C:\\Cadastrei\\clientes_api.json`.
- [ ] Copiar `deploy\\instalar_servicos_nssm.ps1` para `C:\\Cadastrei\\deploy`.

## 6. Validacao Pos-Deploy (sem servico)

Executar ciclo unico antes de instalar/reiniciar servico:

```powershell
C:\Cadastrei\apps\prod\CadastreiApiMotoristasProd\CadastreiApiMotoristasProd.exe --uma-vez --batch-motoristas 1
C:\Cadastrei\apps\prod\CadastreiApiAfastamentosProd\CadastreiApiAfastamentosProd.exe --uma-vez --batch-afastamentos 1
```

## 7. Instalacao/Atualizacao de Servicos (NSSM)

- [ ] Confirmar nome dos servicos.
- [ ] Aplicar script de instalacao/reinstalacao.
- [ ] Validar status `RUNNING`.
- [ ] Validar logs de inicializacao.

## 8. Rollback

## 8.1 Quando acionar
- Erro critico apos deploy.
- Aumento anormal de `ERRO` na fila.
- Nao conformidade funcional.

## 8.2 Como executar
1. Parar servico afetado.
2. Restaurar executavel anterior.
3. Restaurar `clientes_api.json` anterior.
4. Reiniciar servico.
5. Validar saude.

## 9. Checklist de Fechamento

- [ ] Build validado.
- [ ] Deploy concluido.
- [ ] Teste de ciclo unico ok.
- [ ] Servico em execucao.
- [ ] Logs sem erro critico.
- [ ] Evidencia arquivada.

