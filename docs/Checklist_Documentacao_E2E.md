# Checklist E2E de Documentacao

Este checklist organiza a documentacao completa do projeto em duas frentes:
- Visao executiva (Stakeholders e PO)
- Visao tecnica (Desenvolvedores, QA, DevOps e Suporte)

Use este arquivo como "controle mestre" de entrega documental.

---

## 1. Informacoes Basicas da Entrega

- [x] Nome da iniciativa/projeto definido
- [x] Versao da documentacao definida (ex.: `v1.0.0`)
- [x] Autor(es) e responsaveis por aprovacao definidos
- [x] Data de criacao e ultima revisao registradas
- [x] Ambientes cobertos confirmados (`Homologacao` e `Producao`)
- [x] Escopo desta versao documentado (o que entra e o que fica para depois)

### Metadados sugeridos

| Campo | Valor |
|---|---|
| Projeto | Cadastrei Integracao |
| Versao Documento | v1.0 |
| Responsavel Tecnico |  |
| Responsavel Negocio (PO) |  |
| Data |  |
| Status | Rascunho / Em Revisao / Aprovado |

---

## 2. Documentacao Executiva (Stakeholders / PO)

## 2.1 Contexto e Objetivo
- [x] Problema de negocio descrito em linguagem nao tecnica
- [x] Objetivo da integracao (origem Vetorh -> Cadastrei -> API) descrito
- [x] Beneficios esperados (tempo, qualidade, rastreabilidade, compliance) descritos

## 2.2 Escopo Funcional
- [x] Entidades cobertas descritas (`Motorista`, `Afastamento`)
- [x] Origem dos dados descrita (`R034FUN`, `R034CPL`, `R038AFA`, etc.)
- [x] Regras de negocio principais explicadas
- [x] O que nao faz parte do escopo atual documentado

## 2.3 Fluxo de Alto Nivel (E2E)
- [x] Fluxo ponta a ponta desenhado (diagrama simples)
- [x] Pontos de controle definidos (captura, fila, envio, retorno)
- [x] Comportamento em erro explicado (tentativas, status, reprocessamento)
- [x] Comportamento em sucesso explicado (marcacao de enviado, rastreio)

## 2.4 Riscos e Mitigacoes
- [x] Riscos operacionais mapeados (rede, ODBC, indisponibilidade API)
- [x] Riscos de dados mapeados (campo obrigatorio, de-para invalido)
- [x] Mitigacoes e plano de contingencia definidos
- [x] Responsaveis por decisao em incidentes definidos

## 2.5 Governanca e Operacao
- [x] Dono funcional do processo definido
- [x] Janela de suporte e SLA definidos
- [x] Processo de mudanca aprovado (como alterar de-para, endpoint, credenciais)
- [x] Processo de validacao antes de ir para Producao definido

## 2.6 Evidencias para Apresentacao
- [x] Evidencia de execucao em Homologacao
- [x] Evidencia de execucao em Producao controlada
- [x] Evidencia de logs e rastreabilidade (antes/depois, sucesso/erro)

---

## 3. Documentacao Tecnica (Dev / QA / DevOps / Suporte)

## 3.1 Arquitetura Tecnica
- [x] Diagrama tecnico atualizado (componentes, responsabilidades)
- [x] Estrutura de repositorio explicada (`scripts`, `src`, `Interface`, `config`)
- [x] Dependencias e versoes criticas listadas (`Python`, `ODBC`, `PyInstaller`, libs)
- [x] Estrategia de configuracao centralizada explicada (`.env`, `clientes_api.json`)

## 3.2 Modelo de Dados e Contratos
- [ ] Tabelas de integracao documentadas (`MotoristaCadastro`, `Afastamento`)
- [ ] Campos obrigatorios por entidade documentados
- [ ] Regras de status documentadas (`PENDENTE`, `PROCESSANDO`, `ERRO`, `ENVIADO`)
- [ ] Chaves de idempotencia/duplicidade documentadas
- [ ] Dicionarios documentados (`EmpresaDict`, `SindicatoDict`)
- [ ] De-para por endpoint documentado (origem -> destino, obrigatorio/opcional)

## 3.3 Instalacao e Setup de Ambiente
- [x] Pre-requisitos de servidor documentados (rede, porta, ODBC, permissao)
- [x] Checklist de conectividade SQL e API documentado
- [x] Processo de build de executaveis documentado (somente alvo necessario)
- [x] Estrutura de pastas alvo documentada (`C:\\Cadastrei\\...`)
- [x] Processo de deploy por copia documentado

## 3.4 Servicos Windows (NSSM)
- [ ] Nomes de servico por finalidade e ambiente documentados
- [ ] Parametros de instalacao documentados
- [ ] Comandos de start/stop/restart/status documentados
- [ ] Conta de execucao e permissoes documentadas
- [x] Estrategia de rollback documentada

## 3.5 Operacao e Monitoramento
- [x] Local e padrao de logs documentados
- [x] Eventos operacionais importantes documentados
- [x] Checklist semanal de saude documentado
- [x] Alertas recomendados documentados (fila crescendo, erro recorrente, timeout)
- [x] Procedimento de triagem por tipo de erro documentado

## 3.6 Testes (Manual + Tecnico)
- [x] Casos de teste de motorista definidos (sucesso, erro, retries)
- [x] Casos de teste de afastamento definidos
- [x] Casos de teste de de-para definidos (campo faltante, padrao, transformacao)
- [x] Casos de teste de conectividade (SQL/API) definidos
- [x] Criterios de aprovacao para homologacao e producao definidos

## 3.7 Manutencao e Evolucao
- [ ] Como adicionar novo endpoint documentado
- [ ] Como adicionar novo de-para por cliente documentado
- [ ] Como incluir novas colunas sem quebrar compatibilidade documentado
- [ ] Como reprocessar filas com seguranca documentado
- [ ] Plano de versionamento de configuracao/documentacao documentado

## 3.8 Seguranca e Compliance
- [ ] Politica de credenciais documentada (nao versionar segredo em git publico)
- [ ] Permissoes minimas de banco e servico documentadas
- [ ] Mascaramento/controle de dados sensiveis em logs documentado
- [ ] Trilha de auditoria minima documentada (quem alterou endpoint/de-para)

---

## 4. SQL Checklist (Banco e Operacao)

## 4.1 Estrutura e Integridade
- [x] Tabelas obrigatorias existem e estao no schema correto (`dbo`)
- [x] Colunas obrigatorias para integracao existem
- [x] Constraints/indices necessarios conferidos
- [x] Dicionarios (`EmpresaDict`, `SindicatoDict`) populados

## 4.2 Saude da Fila
- [x] Consulta de volume por `Status` validada
- [x] Itens com `ERRO` classificados por `UltimoErro`
- [x] Locks expirados revisados e liberados quando necessario
- [x] Processo de reprocessamento documentado e testado

## 4.3 Performance Basica
- [x] Tempo de captura e envio por lote medido
- [x] Batch size adequado validado por ambiente
- [x] Querys criticas revisadas com plano de execucao (quando necessario)

---

## 5. Checklist de Go-Live (Producao)

- [x] Build final aprovado e versionado
- [x] Deploy realizado em servidor alvo
- [x] `.env` final validado em Producao
- [x] `clientes_api.json` final validado em Producao
- [x] Servicos API instalados e iniciados
- [x] Servicos de captura (motorista/afastamento) validados
- [x] Teste controlado (batch 1) executado com sucesso
- [x] Monitoramento de 30-60 minutos sem erro critico
- [x] Plano de rollback pronto e testado
- [ ] Aprovacao final de PO/Stakeholders registrada

---

## 6. Entregaveis Minimos (Definition of Done da Documentacao)

- [ ] Documento Executivo aprovado
- [ ] Documento Tecnico aprovado
- [ ] Runbook operacional aprovado
- [ ] Checklist SQL e reprocessamento aprovado
- [ ] Checklist de Go-Live aprovado
- [ ] Evidencias anexadas (prints, logs, comandos, resultados)

---

## 7. Estrutura Recomendada de Arquivos

Use esta estrutura para manter padrao e escalabilidade:

```text
docs/
  01_visao_executiva.md
  02_arquitetura_tecnica.md
  03_setup_build_deploy.md
  04_operacao_monitoramento.md
  05_runbook_incidentes.md
  06_sql_operacional.md
  07_testes_homologacao.md
  08_go_live_producao.md
  anexos/
    evidencias/
    diagramas/
```
