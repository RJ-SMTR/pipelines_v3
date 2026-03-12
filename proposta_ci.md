CI de Documentação Automatizada

Arquitetura Viva da Plataforma de Dados
Versão
v1.3 – Fase 1 consolidada
Responsável pela implementação
Engenharia de Dados

1. Objetivo
   Implementar um workflow de documentação automatizada que mantenha a documentação da plataforma de dados alinhada com a evolução de dois repositórios fonte.
   A solução deve:
   analisar mudanças no código
   gerar um snapshot estrutural (graph.json)
   comparar a arquitetura antes e depois da mudança
   analisar a documentação existente
   usar Claude para propor atualizações na documentação
   atualizar duas visões documentais:
   public
   tech
   abrir um único PR no repositório de documentação
   O objetivo é garantir documentação viva da arquitetura, com revisão centralizada e rastreabilidade estrutural.

2. Escopo Inicial
   Componentes envolvidos:
   Repositórios fonte
   repo A (dados)
   repo B (dados)
   Repositório de documentação
   repositório atual de documentação (já existente)
   Saídas do workflow
   graph.json
   atualização da documentação public
   atualização da documentação tech
   PR automático único no repo de docs

3. Organização da Documentação
   O repositório de documentação manterá duas visões editoriais:
   public
   Documentação institucional e metodológica da plataforma.
   tech
   Documentação técnica detalhada da arquitetura.
   Essa separação é editorial, não necessariamente de controle de acesso.

4. Públicos-Alvo
   Documentação public
   Público-alvo:
   área estratégica de outros municípios
   pesquisadores
   órgãos de controle
   concessionárias
   equipes técnicas externas
   Objetivo:
   explicar a arquitetura da plataforma de forma clara e tecnicamente sólida, sem exigir conhecimento interno profundo.
   Conteúdo típico:
   visão geral da plataforma
   principais componentes
   produtos de dados
   relações entre sistemas
   metodologia
   Não incluir:
   detalhes operacionais
   troubleshooting
   infraestrutura interna
   comandos de manutenção

Documentação tech
Público-alvo:
engenharia de dados
operação da plataforma
revisores técnicos internos
Objetivo:
documentar dependências, estrutura e decisões arquiteturais da plataforma.
Conteúdo típico:
dependências entre componentes
fluxos de dados
acoplamentos relevantes
impacto técnico de mudanças

5. Princípios da Solução
   GitHub-first
   Toda a automação ocorre via GitHub Actions.

Um único artefato estrutural
O snapshot arquitetural será representado por:
graph.json

Um único repositório de documentação
O repositório de docs existente será a base oficial.

Um único PR por execução
Todas as alterações propostas serão revisadas em um único pull request.

Claude orienta mudanças, não gera relatórios
O Claude deve propor alterações diretamente nos arquivos de documentação.

6. Fluxo Geral do Workflow
   Trigger:
   push na branch main
   O workflow será executado em cada um dos dois repositórios fonte.
   Fluxo:
   identificar qual repositório disparou o evento
   analisar o código alterado
   executar GitNexus no repositório fonte
   gerar o grafo estrutural
   exportar graph.json
   clonar o repositório de documentação
   executar GitNexus também no repositório de documentação
   comparar o graph.json anterior com o novo
   montar contexto para Claude
   Claude propõe alterações em public e tech
   atualizar os arquivos no repo de docs
   abrir um único PR

7. Uso do GitNexus
   O GitNexus será utilizado como motor estrutural.
   Ele será executado em dois contextos:
   7.1 No repositório fonte
   Objetivo:
   gerar o grafo estrutural do código
   identificar componentes e dependências
   produzir o snapshot graph.json
   Esse grafo servirá como base objetiva para detectar mudanças arquiteturais.

7.2 No repositório de documentação
Objetivo:
gerar uma representação estrutural da documentação existente.
Isso permite:
reduzir perda de contexto ao usar Claude
identificar melhor páginas relevantes
entender como a documentação atual está organizada
O grafo da documentação será usado apenas como contexto, não precisa ser versionado nesta fase.

8. Papel do KuzuDB
   O KuzuDB é utilizado internamente pelo GitNexus como banco de grafo.
   Nesta fase:
   o banco é temporário
   não será persistido
   não será versionado
   Fluxo:
   GitNexus → KuzuDB (temporário) → export → graph.json
   O único artefato estrutural persistido será graph.json.

9. Estrutura do graph.json
   O graph.json representa o estado estrutural do repositório.
   Estrutura sugerida:
   {
   "repository": "repo-name",
   "commit_sha": "abc123",
   "nodes": [
   {
   "id": "node_id",
   "type": "module|dataset|model|pipeline|flow",
   "name": "string"
   }
   ],
   "edges": [
   {
   "source": "node_id",
   "target": "node_id",
   "type": "dependency"
   }
   ]
   }
   Requisitos:
   ordenação determinística
   ausência de campos voláteis
   consistência entre execuções

10. Estratégia de Comparação
    O workflow deve:
    buscar o graph.json atual no repositório de documentação
    gerar um novo graph.json
    comparar os dois arquivos
    Se não houver diferença:
    o workflow pode encerrar
    Se houver diferença:
    a documentação deve ser reavaliada

11. Contexto Enviado ao Claude
    O Claude deve receber como contexto:
    Mudanças no código
    lista de arquivos alterados
    diff resumido

Mudança estrutural
graph_before.json
graph_after.json

Documentação existente
arquivos relevantes em public
arquivos relevantes em tech

12. Papel do Claude
    O Claude deve:
    analisar as mudanças no código
    analisar as mudanças no grafo
    analisar a documentação existente
    propor atualizações na documentação
    Ele deve responder:
    quais páginas precisam ser alteradas
    quais trechos precisam ser atualizados
    se novas páginas devem ser criadas
    O resultado esperado é a atualização dos próprios arquivos de documentação.
    Não deve ser gerado um relatório separado.

13. Prompt para Atualização da Documentação public
    Instrução ao modelo:
    Você está atualizando a documentação pública de uma plataforma de dados.
    Público-alvo:
    área estratégica de outros municípios
    pesquisadores
    órgãos de controle
    concessionárias
    equipes técnicas externas
    Considere:
    mudanças no código
    mudanças no grafo estrutural
    conteúdo atual da documentação pública
    Proponha as alterações necessárias para manter a documentação alinhada à arquitetura atual.
    Priorize:
    visão geral clara
    explicação dos componentes
    relações entre sistemas
    consistência com o que já existe
    Evite:
    detalhes operacionais internos
    troubleshooting
    infraestrutura interna
    Tom:
    institucional, claro e tecnicamente sólido.

14. Prompt para Atualização da Documentação tech
    Você está atualizando a documentação técnica da arquitetura de uma plataforma de dados.
    Considere:
    mudanças no código
    mudanças no grafo estrutural
    documentação técnica existente
    Proponha as alterações necessárias.
    Priorize:
    dependências entre componentes
    impacto das mudanças
    coerência arquitetural
    precisão técnica
    Tom:
    técnico, direto e objetivo.

15. Estrutura no Repositório de Documentação
    Artefatos estruturais:
    generated/
    repo-a/
    graph.json
    repo-b/
    graph.json
    Documentação editorial existente:
    public/
    tech/
    O workflow deve atualizar a documentação existente, não recriá-la.

16. Pull Request Automático
    O workflow deve abrir um único PR no repositório de documentação.
    Esse PR deve conter:
    atualização do graph.json
    alterações na documentação public
    alterações na documentação tech
    Ferramenta recomendada:
    peter-evans/create-pull-request

17. Critérios de Aceite
    A solução será considerada implementada quando:
    o workflow executar em push na main
    o graph.json for gerado corretamente
    o workflow conseguir detectar mudanças estruturais
    o Claude receber contexto adequado
    a documentação public for atualizada automaticamente
    a documentação tech for atualizada automaticamente
    um único PR for aberto no repositório de docs

18. Evoluções Futuras
    Possíveis evoluções:
    grafo consolidado entre os dois repositórios
    visão arquitetural global
    consultas estruturais via banco de grafo persistente
    MCP centralizado usando graph.json
    análise histórica da arquitetura

Se quiser, posso agora te entregar também o desenho da GitHub Action (YAML) que implementa exatamente esse fluxo.
