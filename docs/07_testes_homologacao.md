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
| M-01 | Insert motorista | Novo registro origem | Evento em fila `PENDENTE` e envio `ENVIADO` | Teste realizado 24/02/2025. Inserido registro NUMEMP=152, NUMCAD=4032. Gerado IdFila=531 com Status=PENDENTE → ENVIADO. Resposta API HTTP 200. Print anexo + query validada em Cadastrei.dbo.MotoristaCadastro.|
| M-02 | Update motorista | Alteracao em campo relevante | Novo evento e envio | Alterado campo OrgaoExpedidorDoRG de "SSPAL" para "SSP". IdFila=532 criado. Status final ENVIADO. Log API com retorno sucesso. |
| M-03 | Update reversao | Volta ao valor anterior | Evento de reversao enviado | Campo revertido para valor original. Evento gerado alterado o nome de "MIGUEL ARCANJO ARAUJO SOARES 1" e revertido para "MIGUEL ARCANJO ARAUJO SOARES". IdFila=481. Status=ENVIADO. Payload validado comparando HashPayload anterior. |
| M-04 | Campo obrigatorio faltante | Mapeamento invalido | `ERRO` com mensagem clara | Campo empregador removido intencionalmente na origem. Trigger gerou registro IdFila=411 com Status=ERRO. Mensagem: "item 1º: é obrigatório preencher os campos do empregador". Envio realizado e retornado erro. |
| M-05 | Reprocessamento | Item em `ERRO` | Reenvio com sucesso apos ajuste |Campo empregador removido intencionalmente na origem. Trigger gerou registro IdFila=490 com Status=ERRO. Mensagem: "item 1º: é obrigatório preencher os campos do empregador". Envio realizado e retornado erro, corrigido e Enviado com sucesso na 2º tentativa. |

## 5. Casos de Teste - Afastamentos

| ID | Caso | Entrada | Resultado esperado | Evidencia |
|---|---|---|---|---|
| A-01 | Insert afastamento valido | Novo registro origem | Evento em fila e envio `ENVIADO` | Inserido registro DataDoAfastamento=21/02/2026 Situacao=27. IdFila=2095 criado. Status=PENDENTE → ENVIADO. API respondeu HTTP 200. |
| A-02 | De-para descricao | `payload.descricaodasituacao` | Campo destino preenchido corretamente | Campo Situacao=02 trouxe DescricaoDaSituacao='Férias', IdFilas 2103. Payload validado com descricaodasituacao='Férias'. Conferido no destino com campo correto. |
| A-03 | Campo obrigatorio faltante | Regra obrigatoria sem valor | `ERRO` com rastreabilidade | Campo Colaborador nulo. Registro gerado com Status=ERRO. Mensagem rastreável em UltimoErro. Sem envio à API.|
| A-04 | Retry | Falha temporaria API | Tentativas + proxima tentativa |
Simulada falha API (HTTP 500). Tentativas incrementadas (Tentativas=1). ProximaTentativaEm preenchida corretamente (+5min). Novo envio bem sucedido na 2ª tentativa. |
| A-05 | Reprocessamento manual | Ajuste de configuracao | Item volta para `PENDENTE` e envia | Ajuste realizado em configuração da URL API. Item atualizado para Status='PENDENTE'. Envio realizado com sucesso. |

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


