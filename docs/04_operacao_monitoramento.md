# Operacao e Monitoramento

## 1. Objetivo

Definir rotina operacional diaria para manter os servicos saudaveis, detectar falhas cedo e reduzir MTTR.

## 2. Inventario de Servicos

| Tipo | Homologacao | Producao |
|---|---|---|
| Sync Motoristas | CadastreiMotoristasHom | CadastreiMotoristasProd |
| Sync Afastamentos | CadastreiAfastamentosHom | CadastreiAfastamentosProd |
| API Motoristas | CadastreiApiMotoristasHom / unico | CadastreiApiMotoristasProd / unico |
| API Afastamentos | CadastreiApiAfastamentosHom / unico | CadastreiApiAfastamentosProd / unico |

## 3. Logs Operacionais

### Local padrao
- `C:\\Cadastrei\\logs\\`

### Arquivos esperados
- `motoristas_*.log`
- `afastamentos_*.log`
- `api_motoristas_*.log`
- `api_afastamentos_*.log`
- `*_nssm.out.log`
- `*_nssm.err.log`

## 4. Checklist Diario

- [ ] Todos os servicos esperados estao `RUNNING`.
- [ ] Nao ha crescimento anormal de erros no log.
- [ ] Fila nao esta acumulando pendencias sem processamento.
- [ ] Endpoints da API respondendo no tempo esperado.
- [ ] Sem lock preso recorrente.

## 5. Indicadores Operacionais

| Indicador | Alvo | Fonte |
|---|---|---|
| `ERRO` por hora | | Banco + logs |
| Tempo medio por ciclo | | Logs |
| Itens pendentes antigos | | Banco |
| Tentativas medias por item | | Banco |

## 6. Alarmes Recomendados

- [ ] Servico parado.
- [ ] Falha de conexao com banco/API repetitiva.
- [ ] Fila `PENDENTE` acima de limite.
- [ ] Fila `ERRO` acima de limite.
- [ ] Tempo de ciclo acima de limite.

## 7. Tarefas Semanais

- [ ] Revisar configuracoes de de-para ativas.
- [ ] Revisar crescimento de logs e politica de limpeza.
- [ ] Revisar erros mais frequentes e plano de melhoria.
- [ ] Revisar capacidade de batch/timeout.

## 8. Evidencias de Operacao

- [ ] Captura de status dos servicos.
- [ ] Amostra de logs (inicio, ciclo, erro, recuperacao).
- [ ] Relatorio de fila por status.
- [ ] Registro de incidentes e acoes tomadas.

