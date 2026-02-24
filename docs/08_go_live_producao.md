# Go-Live Producao

## 1. Objetivo

Executar a entrada em producao com controle, validacao e rollback pronto.

## 2. Janela e Responsaveis

| Item | Valor |
|---|---|
| Data/Hora da janela | |
| Duracao prevista | |
| Lider tecnico | |
| Responsavel PO | |
| Responsavel Operacoes | |
| Canal de comunicacao | |

## 3. Checklist Pre Go-Live

- [ ] Homologacao aprovada.
- [ ] Executaveis finais gerados e validados.
- [ ] `.env` final revisado.
- [ ] `clientes_api.json` final revisado.
- [ ] Backup de configuracoes atuais salvo.
- [ ] Plano de rollback revisado com equipe.
- [ ] Stakeholders informados.

## 4. Passo a Passo de Execucao

1. Parar servicos alvo.
2. Copiar executaveis novos.
3. Copiar `.env` e `clientes_api.json`.
4. Executar teste `--uma-vez` com `batch 1`.
5. Validar status em banco e logs.
6. Iniciar servicos.
7. Monitorar primeiros ciclos.

## 5. Validacoes Pos Implantacao

- [ ] Servicos `RUNNING`.
- [ ] Sem erro critico nos logs.
- [ ] Fila processando normalmente.
- [ ] Primeiros eventos `ENVIADO` em motorista.
- [ ] Primeiros eventos `ENVIADO` em afastamento.
- [ ] KPI inicial dentro do esperado.

## 6. Plano de Rollback

### Gatilhos
- [ ] Falha de envio massiva.
- [ ] Erro funcional bloqueante.
- [ ] Instabilidade operacional.

### Procedimento
1. Parar servicos impactados.
2. Restaurar executaveis anteriores.
3. Restaurar configuracoes anteriores.
4. Reiniciar servicos.
5. Validar normalizacao.
6. Comunicar rollback e causa.

## 7. Hypercare (Primeiras 24h/72h)

- [ ] Monitoramento reforcado.
- [ ] Triagem de erros em tempo reduzido.
- [ ] Reuniao rapida de status.
- [ ] Consolidacao de aprendizados.

## 8. Encerramento de Go-Live

- [ ] Relatorio final de implantacao emitido.
- [ ] Pendencias abertas e priorizadas.
- [ ] Aprovacao formal de PO e TI registrada.

## 9. Evidencias Obrigatorias

- [ ] Comandos executados.
- [ ] Logs de inicializacao e ciclo.
- [ ] Prints de status de servicos.
- [ ] Evidencias de eventos enviados.
- [ ] Registro de aprovacao final.

