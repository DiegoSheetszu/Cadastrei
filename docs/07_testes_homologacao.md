# Plano de Testes - Homologacao

## 1. Objetivo

Validar fluxo funcional e tecnico em homologacao antes de promover qualquer mudanca para producao.

## 2. Escopo de Testes

- Captura de `Motorista`.
- Captura de `Afastamento`.
- Envio API de `Motorista`.
- Envio API de `Afastamento`.
- De-para por endpoint.
- Comportamento de erro, retry e reprocessamento.

## 3. Preparacao

- [ ] Banco de homologacao disponivel.
- [ ] API homologacao acessivel.
- [ ] `.env` homologacao validado.
- [ ] `clientes_api.json` homologacao validado.
- [ ] Servicos homologacao parados para teste controlado (quando aplicavel).

## 4. Casos de Teste - Motoristas

| ID | Caso | Entrada | Resultado esperado | Evidencia |
|---|---|---|---|---|
| M-01 | Insert motorista | Novo registro origem | Evento em fila `PENDENTE` e envio `ENVIADO` | |
| M-02 | Update motorista | Alteracao em campo relevante | Novo evento e envio | |
| M-03 | Update reversao | Volta ao valor anterior | Evento de reversao enviado | |
| M-04 | Campo obrigatorio faltante | Mapeamento invalido | `ERRO` com mensagem clara | |
| M-05 | Reprocessamento | Item em `ERRO` | Reenvio com sucesso apos ajuste | |

## 5. Casos de Teste - Afastamentos

| ID | Caso | Entrada | Resultado esperado | Evidencia |
|---|---|---|---|---|
| A-01 | Insert afastamento valido | Novo registro origem | Evento em fila e envio `ENVIADO` | |
| A-02 | De-para descricao | `payload.descricaodasituacao` | Campo destino preenchido corretamente | |
| A-03 | Campo obrigatorio faltante | Regra obrigatoria sem valor | `ERRO` com rastreabilidade | |
| A-04 | Retry | Falha temporaria API | Tentativas + proxima tentativa | |
| A-05 | Reprocessamento manual | Ajuste de configuracao | Item volta para `PENDENTE` e envia | |

## 6. Casos de Teste - Operacao

| ID | Caso | Procedimento | Resultado esperado |
|---|---|---|---|
| O-01 | Servico start/stop | Comandos de servico | Estado muda corretamente |
| O-02 | Log de ciclo | Executar ciclo unico | Registro em log com totais |
| O-03 | Queda de API | Bloquear endpoint | Erro controlado sem travar servico |
| O-04 | Conexao SQL indisponivel | Simular indisponibilidade | Erro rastreavel + recuperacao apos retorno |

## 7. Criterios de Aprovacao

- [ ] 100% dos casos criticos aprovados.
- [ ] Sem erro bloqueante aberto.
- [ ] Evidencias anexadas para cada caso.
- [ ] Aprovacao PO + Tecnico registrada.

## 8. Evidencias Minimas

- [ ] Print da interface (config e monitoramento).
- [ ] Trecho de log por caso testado.
- [ ] Resultado SQL (antes/depois) dos itens testados.
- [ ] Payload/response de API para casos principais.

