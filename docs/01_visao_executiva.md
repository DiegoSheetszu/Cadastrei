# Visao Executiva - Cadastrei Integracao

## 1. Metadados

| Campo | Valor |
|---|---|
| Projeto | Cadastrei Integracao Vetorh -> Cadastrei -> API |
| Versao Documento | 1.0 |
| Data | 25/02/2026 |
| Responsavel Tecnico | João Lucas/Diego Ferreira |
| Responsavel Negocio (PO) | Marcelo Harley |
| Status | Aprovado [ ]|

## 2. Resumo Executivo


- O problema é que acontece cadastro manual das informações no sistema ATS, sendo que os cadastros são realizados no sistema HCM. Os cadastros são os eventos de afastamento e motoristas.
- O objetivo é que novos cadastros e alterações realizadas no sistema HCM sejam integrados com o sistema ATS, assim automatizando os cadastros na ATS e também permitir que sejam cadastrados novos sistemas para integração destas informações com outros clientes/fornecedores.
- O impacto esperado é que o processo se torne mais confiável e assertivo, com as informações sendo cadastradas em tempo real, evitando problemas jurídicos e operacionais. Permitindo também escalabilidade para integração com outros clientes/fornecedores reduzindo custo e tempo.

## 3. Objetivos de Negocio

- [x] Reduzir tempo de integracao de dados entre HCM e API.
- [x] Mitigar erro operacional manual.
- [x] Padronizar onboarding de novos clientes/API.

## 4. Escopo da Solucao

- Integracao de `Motorista`.
- Integracao de `Afastamento`.
- Fila de integracao no banco `Cadastrei`.
- Envio para API com de-para configuravel por cliente/endpoint.
- Interface para operacao e monitoramento.


## 5. Riscos e Mitigacoes

| Risco | Impacto | Probabilidade | Mitigacao | Dono |
|---|---|---|---|---|
| Indisponibilidade API | Alto | Baixa | Retry + monitoramento + contingencia | João Lucas/Diego |
| Falha de conectividade SQL/ODBC | Alto | Baixa | Validacao de driver/rede + fallback | João Lucas/Diego |
| De-para invalido | Médio | Média | Validacao em homologacao + checklist | João Lucas/Diego |
| Crescimento de fila em erro | Baixo | Média | Rotina de triagem e reprocessamento | João Lucas/Diego |

## 6. Governanca

| Papel | Nome | Responsabilidade |
|---|---|---|
| Stakeholder | Fabricia | Patrocinadora do projeto. Define direcionamento estratégico, valida entregas-chave, aprova escopo macro. Responsável por garantir alinhamento do projeto com os objetivos da área. |
| PO | Marcelo Harley | Responsável pelo backlog do produto, priorização das demandas, definição de requisitos funcionais e validação das entregas junto aos usuários finais.|
| Operacao/Infra | João Lucas/Diego | Responsáveis pela arquitetura técnica, infraestrutura, integrações, segurança, disponibilidade, deploy e sustentação da aplicação. |
| Suporte | João Lucas/Diego  | Responsáveis pelo atendimento de incidentes, tratativa de erros, monitoramento pós-implantação e garantia de continuidade operacional. |

## 7. Cronograma Macro

| Marco | Data alvo | Status |
|---|---|---|
| Homologacao concluida | 20/02/2026 | Concluído |
| UAT concluido | 24/02/2026 | Concluído |
| Go-Live Producao | 27/02/2026 | Pendente |

## 8. Criterios de Aceite Executivos

- [x] Fluxo de motorista funcionando fim a fim.
- [x] Fluxo de afastamento funcionando fim a fim.
- [x] Evidencias de homologacao apresentadas.
- [x] Processo de operacao e suporte definido.

## 9. Aprovacoes

| Area | Responsavel | Data | Aprovado (S/N) |
|---|---|---|---|
| Negocio/PO | Marcelo Harley | 10/02/2026 | S |
| TI Desenvolvimento | Diego Ferreira | 10/02/2026 | S |
| TI Desenvolvimento | João Oliveira | 10/02/2026 | S |

