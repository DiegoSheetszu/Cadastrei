# Arquitetura Tecnica - Cadastrei Integracao

## 1. Metadados

| Campo | Valor |
|---|---|
| Versao | v1.0 |
| Data | 25/02/2026 |
| Autor | João Lucas |
| Revisores | Marcelo Harley e Diego Ferreira|

## 2. Contexto Tecnico

Este documento descreve:
- Componentes da integracao.
- Fluxos de dados.
- Contratos de dados.
- Regras tecnicas de operacao.

## 3. Componentes

| Componente | Tipo | Responsabilidade |
|---|---|---|
| Vetorh_Hom / Vetorh_Prod | Banco origem | Fonte de eventos/colunas de RH |
| Cadastrei (dbo) | Banco destino/fila | Persistencia de eventos e status |
| Servico Sync Motoristas | Worker | Captura e grava fila de motoristas |
| Servico Sync Afastamentos | Worker | Captura e grava fila de afastamentos |
| Servico API Motoristas | Worker | Consome fila e envia para endpoint motorista |
| Servico API Afastamentos | Worker | Consome fila e envia para endpoint afastamento |
| Interface | Operacao | Configuracao, monitoramento, acao manual |
| clientes_api.json | Config | Registry de clientes/endpoints/de-para |
| .env | Config | Parametros de conexao e runtime |

## 4. Fluxo E2E

1. Origem Vetorh atualiza dados.
2. Servicos de Sync capturam e geram payload na fila do Cadastrei.
3. Servicos API leem pendencias, aplicam de-para e enriquecimento.
4. API responde sucesso/erro.
5. Status da fila e evidencias ficam rastreaveis em banco/log.

## 5. Estrutura do Repositorio

| Pasta/Arquivo | Uso |
|---|---|
| `src/integradora/` | Servicos e logica de integracao |
| `Consultas_dbo/` | Repositorios SQL |
| `scripts/` | Entrypoints de servico/build/deploy |
| `Interface/` | UI de operacao |
| `config/` | Settings, engine, registry |
| `.env` | Config runtime |
| `clientes_api.json` | Config de cliente/API/de-para |

## 6. Configuracao e Precedencia

Precedencia recomendada:
1. Argumentos de linha de comando.
2. Variaveis de ambiente do processo.
3. `.env` no diretorio base de execucao.

Observacoes:
- Evitar duplicidade de configuracao por ambiente.
- Garantir consistencia entre `C:\\Cadastrei\\.env` e pastas de executavel.

## 7. Modelo de Dados (Resumo)

### Tabelas principais
- `dbo.MotoristaCadastro`
- `dbo.Afastamento`
- `dbo.EmpresaDict`
- `dbo.SindicatoDict`
- Tabelas de estado/checkpoint/cursor (sync)

### Campos de controle esperados
- `Status`
- `Tentativas`
- `LockId`
- `LockEm`
- `ProximaTentativaEm`
- `UltimoErro`
- `HttpStatus`
- `RespostaResumo`

## 8. Regras Tecnicas Importantes

- Idempotencia de eventos por chave tecnica.
- Retry com backoff para falha temporaria.
- Lock otimista para evitar dupla captura no processamento.
- De-para por endpoint para suportar multiplos clientes.
- Compatibilidade para evolucao de payload e colunas novas.

## 9. Requisitos Nao Funcionais

| Requisito | Diretriz |
|---|---|
| Disponibilidade | Servicos Windows com reinicio controlado |
| Observabilidade | Logs por servico + status em banco |
| Seguranca | Credenciais fora de codigo-fonte |
| Escalabilidade | De-para por endpoint + dicionarios por codigo |
| Manutenibilidade | Scripts padronizados e runbook |

## 10. Decisoes de Arquitetura

| Decisao | Motivo | Tradeoff |
|---|---|---|
| Fila em banco SQL | Rastreabilidade e simplicidade operacional | Maior dependencia do banco |
| Servicos separados por dominio | Isolamento de falha e operacao | Mais itens para monitorar |
| De-para externo | Flexibilidade por cliente | Governanca de configuracao necessaria |

