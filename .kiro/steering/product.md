# SqlMentor

CLI + MCP Server Python que conecta em bancos Oracle 11g+, extrai metadata de SQL (plano de execução, DDLs, índices, estatísticas, constraints, parâmetros do otimizador) e gera relatórios estruturados (Markdown/JSON) otimizados para consumo por LLMs em fluxos de tuning assistido por IA.

O público-alvo são DBAs e desenvolvedores que querem contexto Oracle estruturado para tuning assistido por IA — via CLI no terminal ou via MCP Server integrado ao Kiro/Claude Desktop.

Projeto em estágio inicial (v0.1.0). Suporta apenas Oracle por enquanto. Roadmap inclui MariaDB e cache de metadata.

## Interfaces

- **CLI** (`sqlmentor`): uso direto no terminal, relatórios salvos em `reports/`
- **MCP Server** (`sqlmentor-mcp`): integração com IDEs via Model Context Protocol (stdio)
- **Kiro Power** (`powers/sqlmentor/`): empacota MCP + documentação + metodologia de análise para times que usam Kiro
