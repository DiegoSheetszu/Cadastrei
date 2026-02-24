# Runbook de Incidentes

## 1. Objetivo

Padronizar resposta a incidentes para reduzir tempo de analise e recuperar operacao com seguranca.

## 2. Classificacao

| Severidade | Definicao | Acao |
|---|---|---|
| Sev1 | Impacto total em producao | Acionamento imediato + contingencia |
| Sev2 | Impacto parcial | Correcao prioritaria |
| Sev3 | Impacto baixo | Planejar ajuste |

## 3. Fluxo de Triagem

1. Confirmar servico e ambiente afetado.
2. Identificar erro dominante (log + banco).
3. Classificar severidade.
4. Aplicar acao de contorno.
5. Executar correcao definitiva.
6. Validar e encerrar com evidencia.

## 4. Incidentes Comuns e Respostas

## 4.1 Erro ODBC IM002
Sintoma:
- `Data source name not found and no default driver specified`

Acoes:
- [ ] Confirmar `DB_DRIVER` no `.env`.
- [ ] Validar driver instalado no servidor.
- [ ] Testar conexao com ciclo unico.

## 4.2 Erro de conexao 08001
Sintoma:
- Timeout ou servidor nao acessivel.

Acoes:
- [ ] Validar rede (`Test-NetConnection`).
- [ ] Validar host/porta SQL.
- [ ] Validar firewall e roteamento.

## 4.3 Campo obrigatorio ausente no de-para
Sintoma:
- `Campo obrigatorio ausente no de-para (...)`

Acoes:
- [ ] Revisar `clientes_api.json` efetivo.
- [ ] Validar endpoint e de-para ativos.
- [ ] Ajustar mapeamento e reprocessar fila.

## 4.4 Erro de validacao da API
Sintoma:
- Mensagem de regra de negocio da API.

Acoes:
- [ ] Capturar payload enviado + resposta HTTP.
- [ ] Ajustar de-para/campos obrigatorios.
- [ ] Reprocessar itens em `ERRO`.

## 5. Comandos de Apoio (Windows)

```powershell
sc query CadastreiApiMotoristasProd
sc query CadastreiApiAfastamentosProd
Get-Content C:\Cadastrei\logs\api_motoristas_prod.log -Tail 100
Get-Content C:\Cadastrei\logs\api_afastamentos_prod.log -Tail 100
```

## 6. Reprocessamento Seguro

- [ ] Filtrar somente itens afetados.
- [ ] Limpar lock e resetar tentativas conforme politica.
- [ ] Evitar reprocessamento em massa sem analise.
- [ ] Registrar motivo e horario da acao.

## 7. Comunicacao

| Momento | Mensagem minima |
|---|---|
| Abertura | Ambiente, impacto, horario, severidade |
| Atualizacao | Acao executada, resultado parcial, proximo passo |
| Encerramento | Causa raiz, correcao, evidencias, prevencao |

## 8. Pos-Incidente

- [ ] Registrar causa raiz.
- [ ] Registrar acao corretiva/preventiva.
- [ ] Atualizar documentacao.
- [ ] Atualizar monitoramento/checklists.

