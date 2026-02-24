# Checklist E2E de Documentacao

Este checklist organiza a documentacao completa do projeto em duas frentes:
- Visao executiva (Stakeholders e PO)
- Visao tecnica (Desenvolvedores, QA, DevOps e Suporte)

Use este arquivo como "controle mestre" de entrega documental.

---

## 1. Informacoes Basicas da Entrega

- [ ] Nome da iniciativa/projeto definido
- [ ] Versao da documentacao definida (ex.: `v1.0.0`)
- [ ] Autor(es) e responsaveis por aprovacao definidos
- [ ] Data de criacao e ultima revisao registradas
- [ ] Ambientes cobertos confirmados (`Homologacao` e `Producao`)
- [ ] Escopo desta versao documentado (o que entra e o que fica para depois)

### Metadados sugeridos

| Campo | Valor |
|---|---|
| Projeto | Cadastrei Integracao |
| Versao Documento |  |
| Responsavel Tecnico |  |
| Responsavel Negocio (PO) |  |
| Data |  |
| Status | Rascunho / Em Revisao / Aprovado |

---

## 2. Documentacao Executiva (Stakeholders / PO)

## 2.1 Contexto e Objetivo
- [ ] Problema de negocio descrito em linguagem nao tecnica
- [ ] Objetivo da integracao (origem Vetorh -> Cadastrei -> API) descrito
- [ ] Beneficios esperados (tempo, qualidade, rastreabilidade, compliance) descritos
- [ ] KPIs de sucesso definidos (ex.: taxa de envio, tempo medio, erro por tipo)

## 2.2 Escopo Funcional
- [ ] Entidades cobertas descritas (`Motorista`, `Afastamento`)
- [ ] Origem dos dados descrita (`R034FUN`, `R034CPL`, `R038AFA`, etc.)
- [ ] Regras de negocio principais explicadas
- [ ] O que nao faz parte do escopo atual documentado

## 2.3 Fluxo de Alto Nivel (E2E)
- [ ] Fluxo ponta a ponta desenhado (diagrama simples)
- [ ] Pontos de controle definidos (captura, fila, envio, retorno)
- [ ] Comportamento em erro explicado (tentativas, status, reprocessamento)
- [ ] Comportamento em sucesso explicado (marcacao de enviado, rastreio)

## 2.4 Riscos e Mitigacoes
- [ ] Riscos operacionais mapeados (rede, ODBC, indisponibilidade API)
- [ ] Riscos de dados mapeados (campo obrigatorio, de-para invalido)
- [ ] Mitigacoes e plano de contingencia definidos
- [ ] Responsaveis por decisao em incidentes definidos

## 2.5 Governanca e Operacao
- [ ] Dono funcional do processo definido
- [ ] Janela de suporte e SLA definidos
- [ ] Processo de mudanca aprovado (como alterar de-para, endpoint, credenciais)
- [ ] Processo de validacao antes de ir para Producao definido

## 2.6 Evidencias para Apresentacao
- [ ] Prints da interface (configuracao, monitoramento, lista de integracao)
- [ ] Evidencia de execucao em Homologacao
- [ ] Evidencia de execucao em Producao controlada
- [ ] Evidencia de logs e rastreabilidade (antes/depois, sucesso/erro)

---

## 3. Documentacao Tecnica (Dev / QA / DevOps / Suporte)

## 3.1 Arquitetura Tecnica
- [ ] Diagrama tecnico atualizado (componentes, responsabilidades)
- [ ] Estrutura de repositorio explicada (`scripts`, `src`, `Interface`, `config`)
- [ ] Dependencias e versoes criticas listadas (`Python`, `ODBC`, `PyInstaller`, libs)
- [ ] Estrategia de configuracao centralizada explicada (`.env`, `clientes_api.json`)

## 3.2 Modelo de Dados e Contratos
- [ ] Tabelas de integracao documentadas (`MotoristaCadastro`, `Afastamento`)
- [ ] Campos obrigatorios por entidade documentados
- [ ] Regras de status documentadas (`PENDENTE`, `PROCESSANDO`, `ERRO`, `ENVIADO`)
- [ ] Chaves de idempotencia/duplicidade documentadas
- [ ] Dicionarios documentados (`EmpresaDict`, `SindicatoDict`)
- [ ] De-para por endpoint documentado (origem -> destino, obrigatorio/opcional)

## 3.3 Instalacao e Setup de Ambiente
- [ ] Pre-requisitos de servidor documentados (rede, porta, ODBC, permissao)
- [ ] Checklist de conectividade SQL e API documentado
- [ ] Processo de build de executaveis documentado (somente alvo necessario)
- [ ] Estrutura de pastas alvo documentada (`C:\\Cadastrei\\...`)
- [ ] Processo de deploy por copia documentado

## 3.4 Servicos Windows (NSSM)
- [ ] Nomes de servico por finalidade e ambiente documentados
- [ ] Parametros de instalacao documentados
- [ ] Comandos de start/stop/restart/status documentados
- [ ] Conta de execucao e permissoes documentadas
- [ ] Estrategia de rollback documentada

## 3.5 Operacao e Monitoramento
- [ ] Local e padrao de logs documentados
- [ ] Eventos operacionais importantes documentados
- [ ] Checklist diario de saude documentado
- [ ] Alertas recomendados documentados (fila crescendo, erro recorrente, timeout)
- [ ] Procedimento de triagem por tipo de erro documentado

## 3.6 Testes (Manual + Tecnico)
- [ ] Casos de teste de motorista definidos (sucesso, erro, retries)
- [ ] Casos de teste de afastamento definidos
- [ ] Casos de teste de de-para definidos (campo faltante, padrao, transformacao)
- [ ] Casos de teste de conectividade (SQL/API) definidos
- [ ] Criterios de aprovacao para homologacao e producao definidos

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
- [ ] Tabelas obrigatorias existem e estao no schema correto (`dbo`)
- [ ] Colunas obrigatorias para integracao existem
- [ ] Constraints/indices necessarios conferidos
- [ ] Dicionarios (`EmpresaDict`, `SindicatoDict`) populados

## 4.2 Saude da Fila
- [ ] Consulta de volume por `Status` validada
- [ ] Itens com `ERRO` classificados por `UltimoErro`
- [ ] Locks expirados revisados e liberados quando necessario
- [ ] Processo de reprocessamento documentado e testado

## 4.3 Performance Basica
- [ ] Tempo de captura e envio por lote medido
- [ ] Batch size adequado validado por ambiente
- [ ] Querys criticas revisadas com plano de execucao (quando necessario)

---

## 5. Checklist de Go-Live (Producao)

- [ ] Build final aprovado e versionado
- [ ] Deploy realizado em servidor alvo
- [ ] `.env` final validado em Producao
- [ ] `clientes_api.json` final validado em Producao
- [ ] Servicos API instalados e iniciados
- [ ] Servicos de captura (motorista/afastamento) validados
- [ ] Teste controlado (batch 1) executado com sucesso
- [ ] Monitoramento de 30-60 minutos sem erro critico
- [ ] Plano de rollback pronto e testado
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

---

## 8. Proximo Passo Sugerido

- [ ] Preencher este checklist com responsavel e prazo por item
- [ ] Criar os 8 documentos da estrutura recomendada
- [ ] Agendar revisao tecnica + revisao executiva separadas
