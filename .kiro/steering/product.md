# sql-tuner

CLI Python que conecta em bancos Oracle 11g+, extrai metadata de SQL (plano de execução, DDLs, índices, estatísticas, constraints, parâmetros do otimizador) e gera relatórios estruturados (Markdown/JSON) otimizados para consumo por LLMs em fluxos de tuning assistido por IA.

O público-alvo são DBAs e desenvolvedores que querem colar contexto Oracle num chat com IA para receber sugestões de otimização.

Projeto em estágio inicial (v0.1.0). Suporta apenas Oracle por enquanto. Roadmap inclui MariaDB, integração direta com LLMs via flag `--ask`, e um MCP Server.
