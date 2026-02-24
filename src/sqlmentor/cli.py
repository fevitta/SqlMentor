"""
SqlMentor CLI — Coleta de contexto para tuning de SQL assistido por IA.

Uso:
    sqlmentor analyze <arquivo.sql> --conn <profile>
    sqlmentor analyze --sql "SELECT ..." --conn <profile>
    sqlmentor inspect <sql_id> --conn <profile>
    sqlmentor parse <arquivo.sql> --schema <SCHEMA>
    sqlmentor parse --sql "SELECT ..." --schema <SCHEMA>
    sqlmentor config add --name prod --host ... --port 1521 --service ORCL --user ...
    sqlmentor config list
    sqlmentor config test --name prod
    sqlmentor config remove --name prod
"""

import hashlib
import sys
from datetime import datetime
from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

app = typer.Typer(
    name="sqlmentor",
    help="SqlMentor — Coleta contexto Oracle para tuning de SQL assistido por IA.",
    no_args_is_help=True,
)
config_app = typer.Typer(help="Gerencia conexões Oracle.")
app.add_typer(config_app, name="config")

console = Console()


# ═══════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════


def _resolve_sql_input(
    sql_file: Path | None, sql_inline: str | None
) -> tuple[str, str]:
    """Resolve a fonte do SQL: arquivo ou --sql inline. Retorna (texto, label)."""
    if sql_inline and sql_file:
        console.print("[red]Erro:[/red] Passe arquivo OU --sql, não ambos.")
        raise typer.Exit(1)
    if sql_inline:
        if not sql_inline.strip():
            console.print("[red]Erro:[/red] SQL inline está vazio.")
            raise typer.Exit(1)
        return sql_inline, "--sql inline"
    if sql_file:
        if not sql_file.exists():
            console.print(f"[red]Erro:[/red] Arquivo '{sql_file}' não encontrado.")
            raise typer.Exit(1)
        text = sql_file.read_text(encoding="utf-8")
        if not text.strip():
            console.print("[red]Erro:[/red] Arquivo SQL está vazio.")
            raise typer.Exit(1)
        return text, sql_file.name
    console.print("[red]Erro:[/red] Passe um arquivo .sql ou use --sql 'SELECT ...'")
    raise typer.Exit(1)


# ═══════════════════════════════════════════════════════════════════
# ANALYZE
# ═══════════════════════════════════════════════════════════════════


@app.command()
def analyze(
    sql_file: Path = typer.Argument(
        None,
        help="Arquivo .sql com a query/procedure/trigger a ser analisada.",
    ),
    sql: str = typer.Option(
        None, "--sql", help="SQL inline (alternativa ao arquivo)."
    ),
    conn: str = typer.Option(
        None, "--conn", "-c", help="Nome do profile de conexão (usa o default se omitido)."
    ),
    output: Path = typer.Option(
        None, "--output", "-o", help="Arquivo de saída. Se omitido, imprime no stdout."
    ),
    format: str = typer.Option(
        "markdown", "--format", "-f", help="Formato: markdown ou json."
    ),
    schema: str = typer.Option(
        None, "--schema", "-s", help="Schema padrão (sobrescreve o do profile)."
    ),
    deep: bool = typer.Option(
        False, "--deep", "-d", help="Coleta extra: histogramas e partições."
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Imprime o relatório completo no console."
    ),
    expand_views: bool = typer.Option(
        False, "--expand-views", help="Detalha views (DDL, colunas). Sem isso, views aparecem só como referência."
    ),
    expand_functions: bool = typer.Option(
        False, "--expand-functions", help="Coleta DDL de funções PL/SQL referenciadas no SQL."
    ),
    execute: bool = typer.Option(
        False, "--execute", "-x", help="Executa a query real e coleta plano com ALLSTATS LAST + métricas de runtime."
    ),
    bind: list[str] = typer.Option(
        [], "--bind", "-b", help="Bind variables no formato nome=valor (ex: -b idDesconto=123). Repetível."
    ),
    timeout: int = typer.Option(
        None, "--timeout", "-t", help="Timeout em segundos (sobrescreve o do profile, default: 180)."
    ),
    normalized: bool = typer.Option(
        False, "--normalized", "-n", help="SQL normalizado (Datadog, OEM, etc.) — substitui '?' antes do parse. Auto-detectado se omitido."
    ),
    denorm_mode: str = typer.Option(
        "literal", "--denorm-mode", help="Estratégia de desnormalização: 'literal' ('?' → '1') ou 'bind' ('?' → :dn1, :dn2...)."
    ),
) -> None:
    """Analisa um SQL e coleta contexto Oracle para tuning."""
    from sqlmentor.collector import collect_context
    from sqlmentor.connector import connect, get_connection_config, resolve_connection
    from sqlmentor.parser import denormalize_sql, is_normalized_sql, parse_sql
    from sqlmentor.report import to_json, to_markdown

    # Resolve conexão (explícita > default > erro)
    try:
        conn = resolve_connection(conn)
    except ValueError as e:
        console.print(f"[red]Erro:[/red] {e}")
        raise typer.Exit(1)

    # Resolve fonte do SQL
    sql_text, source_label = _resolve_sql_input(sql_file, sql)

    # Auto-detecção de SQL normalizado (Datadog, OEM, etc.)
    if not normalized and is_normalized_sql(sql_text):
        normalized = True
        console.print("[yellow]⚠ SQL normalizado detectado[/yellow] (placeholders '?' de Datadog/OEM). Desnormalizando automaticamente.")

    # Desnormaliza SQL se veio de ferramenta de monitoramento
    if normalized:
        if execute:
            console.print(
                "[red]Erro:[/red] SQL normalizado é incompatível com --execute. "
                "Os literais originais foram perdidos."
            )
            raise typer.Exit(1)
        sql_text, _denorm_binds = denormalize_sql(sql_text, mode=denorm_mode)

    # Resolve schema
    cfg = get_connection_config(conn)
    effective_schema = schema or cfg.get("schema", cfg.get("user", "").upper())

    # Parse
    console.print(f"[cyan]Parsing:[/cyan] {source_label}")
    parsed = parse_sql(sql_text, default_schema=effective_schema)

    console.print(f"  Tipo: [bold]{parsed.sql_type}[/bold]")
    console.print(f"  Tabelas: [bold]{', '.join(parsed.table_names) or 'nenhuma identificada'}[/bold]")

    if parsed.parse_errors:
        console.print(f"  [yellow]⚠ Parse parcial:[/yellow] {'; '.join(parsed.parse_errors)}")

    if not parsed.tables:
        console.print("[yellow]Aviso:[/yellow] Nenhuma tabela identificada. Gerando relatório parcial.")

    # Conecta
    console.print(f"[cyan]Conectando:[/cyan] {conn}")
    try:
        oracle_conn = connect(conn, timeout=timeout)
    except Exception as e:
        console.print(f"[red]Erro de conexão:[/red] {e}")
        raise typer.Exit(1)

    # Parseia bind variables (nome=valor → dict)
    bind_params: dict[str, str] = {}
    for b in bind:
        if "=" not in b:
            console.print(f"[red]Bind inválido:[/red] '{b}' — use formato nome=valor")
            raise typer.Exit(1)
        key, val = b.split("=", 1)
        bind_params[key.strip()] = val.strip()

    # Detecta binds no SQL e reconcilia nomes (case-insensitive)
    import re
    # Captura :param mas ignora ::cast e strings entre aspas
    sql_bind_names = re.findall(r'(?<!:):([A-Za-z_]\w*)', sql_text)
    # Deduplica preservando o case original do SQL (primeiro encontrado)
    seen_upper: set[str] = set()
    unique_sql_binds: list[str] = []
    for name in sql_bind_names:
        if name.upper() not in seen_upper:
            seen_upper.add(name.upper())
            unique_sql_binds.append(name)

    # Remapeia bind_params pro case exato do SQL (Oracle é case-sensitive nos binds)
    if bind_params and unique_sql_binds:
        provided_upper = {k.upper(): v for k, v in bind_params.items()}
        remapped: dict[str, str | int | float | None] = {}
        for sql_name in unique_sql_binds:
            if sql_name.upper() in provided_upper:
                raw_val = provided_upper[sql_name.upper()]
                # Trata null/None como Python None (Oracle NULL)
                if raw_val.lower() in ("null", "none"):
                    remapped[sql_name] = None
                else:
                    # Converte pra número se possível (evita ORA-01790 em UNION ALL)
                    try:
                        remapped[sql_name] = int(raw_val)
                    except ValueError:
                        try:
                            remapped[sql_name] = float(raw_val)
                        except ValueError:
                            remapped[sql_name] = raw_val
        bind_params = remapped

    # Avisa se faltam binds (só relevante com --execute)
    if execute and unique_sql_binds:
        sql_binds_upper = {b.upper() for b in unique_sql_binds}
        provided_upper = {k.upper() for k in bind_params}
        missing = sql_binds_upper - provided_upper
        if missing:
            # Monta comando sugerido com placeholders
            src_arg = str(sql_file) if sql_file else f'--sql "{sql_text[:60]}..."'
            bind_flags = " ".join(
                f"-b {name}=<valor>" if name.upper() in missing
                else f"-b {name}={bind_params.get(name, '')}"
                for name in unique_sql_binds
            )
            suggested = f"sqlmentor analyze {src_arg} --conn {conn} -x {bind_flags}"
            console.print(
                f"[yellow]⚠ Binds não informados:[/yellow] {', '.join(sorted(missing))}\n"
                f"  Execução real desabilitada. Preencha os valores e re-execute:\n"
                f"  [dim]{suggested}[/dim]"
            )
            execute = False

    # Coleta
    console.print("[cyan]Coletando contexto...[/cyan]")
    try:
        ctx = collect_context(
            parsed=parsed,
            conn=oracle_conn,
            default_schema=effective_schema,
            deep=deep,
            expand_views=expand_views,
            expand_functions=expand_functions,
            execute=execute,
            bind_params=bind_params or None,
        )
    except Exception as e:
        console.print(f"[red]Erro na coleta:[/red] {e}")
        raise typer.Exit(1)
    finally:
        oracle_conn.close()

    # Relatório
    if format.lower() == "json":
        report = to_json(ctx)
    else:
        report = to_markdown(ctx)

    if output:
        out_path = output
    else:
        # Pasta padrão: reports/
        reports_dir = Path("reports")
        reports_dir.mkdir(exist_ok=True)

        # Nome: report_<timestamp>_<filename ou hash>.ext
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        ext = "json" if format.lower() == "json" else "md"
        if sql_file:
            base = sql_file.stem
        else:
            base = hashlib.md5(sql_text.encode()).hexdigest()[:8]
        out_path = reports_dir / f"report_{ts}_{base}.{ext}"

    out_path.write_text(report, encoding="utf-8")
    console.print(f"[green]✓[/green] Relatório salvo em [bold]{out_path}[/bold]")

    if verbose:
        console.print("")
        console.print(Panel(report, title="SQL Tuning Context", border_style="blue"))

    # Resumo
    _print_summary(ctx)


# ═══════════════════════════════════════════════════════════════════
# INSPECT
# ═══════════════════════════════════════════════════════════════════


@app.command()
def inspect(
    sql_id: str = typer.Argument(
        ..., help="SQL_ID da query no shared pool Oracle."
    ),
    conn: str = typer.Option(
        None, "--conn", "-c", help="Nome do profile de conexão (usa o default se omitido)."
    ),
    output: Path = typer.Option(
        None, "--output", "-o", help="Arquivo de saída. Se omitido, salva em reports/."
    ),
    format: str = typer.Option(
        "markdown", "--format", "-f", help="Formato: markdown ou json."
    ),
    schema: str = typer.Option(
        None, "--schema", "-s", help="Schema padrão (sobrescreve o do profile)."
    ),
    deep: bool = typer.Option(
        False, "--deep", "-d", help="Coleta extra: histogramas e partições."
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Imprime o relatório completo no console."
    ),
    expand_views: bool = typer.Option(
        False, "--expand-views", help="Detalha views (DDL, colunas)."
    ),
    expand_functions: bool = typer.Option(
        False, "--expand-functions", help="Coleta DDL de funções PL/SQL referenciadas."
    ),
    timeout: int = typer.Option(
        None, "--timeout", "-t", help="Timeout em segundos (sobrescreve o do profile)."
    ),
) -> None:
    """Coleta contexto de um SQL já executado via sql_id (sem re-executar)."""
    from sqlmentor.collector import collect_context
    from sqlmentor.connector import connect, get_connection_config, resolve_connection
    from sqlmentor.parser import parse_sql
    from sqlmentor.queries import runtime_plan, sql_runtime_stats, sql_text_by_id
    from sqlmentor.report import to_json, to_markdown

    # Resolve conexão (explícita > default > erro)
    try:
        conn = resolve_connection(conn)
    except ValueError as e:
        console.print(f"[red]Erro:[/red] {e}")
        raise typer.Exit(1)

    # Resolve schema
    cfg = get_connection_config(conn)
    effective_schema = schema or cfg.get("schema", cfg.get("user", "").upper())

    # Conecta
    console.print(f"[cyan]Conectando:[/cyan] {conn}")
    try:
        oracle_conn = connect(conn, timeout=timeout)
    except Exception as e:
        console.print(f"[red]Erro de conexão:[/red] {e}")
        raise typer.Exit(1)

    cursor = oracle_conn.cursor()

    # Recupera SQL original do shared pool
    console.print(f"[cyan]Buscando SQL_ID:[/cyan] {sql_id}")
    try:
        sql_query, params = sql_text_by_id(sql_id)
        cursor.execute(sql_query, params)
        row = cursor.fetchone()
        if not row or not row[0]:
            console.print(f"[red]Erro:[/red] SQL_ID '{sql_id}' não encontrado no shared pool (V$SQL).")
            console.print("  O cursor pode ter sido expurgado. Tente re-executar a query.")
            oracle_conn.close()
            raise typer.Exit(1)
        sql_text = str(row[0]).read() if hasattr(row[0], "read") else str(row[0])
    except Exception as e:
        if "não encontrado" in str(e) or "Exit" in type(e).__name__:
            raise
        console.print(f"[red]Erro ao buscar SQL:[/red] {e}")
        oracle_conn.close()
        raise typer.Exit(1)

    console.print(f"[green]✓[/green] SQL recuperado ({len(sql_text)} chars)")

    # Parse
    parsed = parse_sql(sql_text, default_schema=effective_schema)
    console.print(f"  Tipo: [bold]{parsed.sql_type}[/bold]")
    console.print(f"  Tabelas: [bold]{', '.join(parsed.table_names) or 'nenhuma'}[/bold]")

    # Coleta plano real via sql_id (sem re-executar)
    console.print("[cyan]Coletando plano real...[/cyan]")
    try:
        sql_query, params = runtime_plan(sql_id)
        cursor.execute(sql_query, params)
        runtime_plan_lines = [r[0] for r in cursor]
    except Exception as e:
        console.print(f"[yellow]⚠ Plano real não disponível:[/yellow] {e}")
        runtime_plan_lines = None

    # Coleta métricas de V$SQL
    console.print("[cyan]Coletando métricas V$SQL...[/cyan]")
    try:
        sql_query, params = sql_runtime_stats(sql_id)
        cursor.execute(sql_query, params)
        columns = [col[0].lower() for col in cursor.description or []]
        row = cursor.fetchone()
        runtime_stats = dict(zip(columns, row)) if row else None
    except Exception as e:
        console.print(f"[yellow]⚠ Métricas V$SQL não disponíveis:[/yellow] {e}")
        runtime_stats = None

    cursor.close()

    # Coleta metadata das tabelas (fluxo normal, sem execute)
    console.print("[cyan]Coletando contexto das tabelas...[/cyan]")
    try:
        ctx = collect_context(
            parsed=parsed,
            conn=oracle_conn,
            default_schema=effective_schema,
            deep=deep,
            expand_views=expand_views,
            expand_functions=expand_functions,
            execute=False,
        )
    except Exception as e:
        console.print(f"[red]Erro na coleta:[/red] {e}")
        raise typer.Exit(1)
    finally:
        oracle_conn.close()

    # Injeta plano real e métricas coletados via sql_id
    if runtime_plan_lines:
        ctx.runtime_plan = runtime_plan_lines
    if runtime_stats:
        ctx.runtime_stats = runtime_stats

    # Relatório
    if format.lower() == "json":
        report = to_json(ctx)
    else:
        report = to_markdown(ctx)

    if output:
        out_path = output
    else:
        reports_dir = Path("reports")
        reports_dir.mkdir(exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        ext = "json" if format.lower() == "json" else "md"
        out_path = reports_dir / f"report_{ts}_inspect_{sql_id}.{ext}"

    out_path.write_text(report, encoding="utf-8")
    console.print(f"[green]✓[/green] Relatório salvo em [bold]{out_path}[/bold]")

    if verbose:
        console.print("")
        console.print(Panel(report, title="SQL Tuning Context", border_style="blue"))

    _print_summary(ctx)


def _print_summary(ctx) -> None:
    """Imprime resumo da coleta."""
    console.print("")
    table = Table(title="Resumo da Coleta", show_header=True)
    table.add_column("Item", style="cyan")
    table.add_column("Status", justify="center")

    table.add_row(
        "Execution Plan",
        "[green]✓[/green]" if ctx.execution_plan else "[yellow]—[/yellow]",
    )

    if ctx.runtime_plan:
        table.add_row(
            "Runtime Plan (ALLSTATS LAST)",
            "[green]✓[/green]",
        )
    if ctx.runtime_stats:
        table.add_row(
            "Runtime Stats (V$SQL)",
            "[green]✓[/green]",
        )
    if ctx.wait_events:
        table.add_row(
            "Wait Events",
            f"[green]{len(ctx.wait_events)}[/green]",
        )

    for t in ctx.tables:
        is_view = t.object_type == "VIEW"
        obj_label = "View" if is_view else "Tabela"
        expanded = bool(t.ddl or t.columns)

        if is_view and not expanded:
            # View não expandida — linha única, sem sub-itens
            table.add_row(
                f"View {t.schema}.{t.name}",
                "[dim]skip[/dim]",
            )
            continue

        table.add_row(
            f"{obj_label} {t.schema}.{t.name}",
            "[green]✓[/green]",
        )
        table.add_row(
            f"  DDL",
            "[green]✓[/green]" if t.ddl else "[red]✗[/red]",
        )
        if not is_view:
            table.add_row(
                f"  Stats",
                "[green]✓[/green]" if t.stats else "[red]✗[/red]",
            )
        table.add_row(
            f"  Colunas",
            f"[green]{len(t.columns)}[/green]" if t.columns else "[red]✗[/red]",
        )
        table.add_row(
            f"  Índices",
            f"[green]{len(t.indexes)}[/green]" if t.indexes else "[yellow]0[/yellow]",
        )
        table.add_row(
            f"  Constraints",
            f"[green]{len(t.constraints)}[/green]" if t.constraints else "[yellow]0[/yellow]",
        )

    table.add_row(
        "Optimizer Params",
        f"[green]{len(ctx.optimizer_params)}[/green]" if ctx.optimizer_params else "[red]✗[/red]",
    )

    if ctx.errors:
        table.add_row("Erros", f"[red]{len(ctx.errors)}[/red]")

    console.print(table)


# ═══════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════


@config_app.command("add")
def config_add(
    name: str = typer.Option(..., "--name", "-n", help="Nome do profile."),
    host: str = typer.Option(..., "--host", "-h", help="Host do Oracle."),
    port: int = typer.Option(1521, "--port", "-p", help="Porta."),
    service: str = typer.Option(..., "--service", "-s", help="Service name."),
    user: str = typer.Option(..., "--user", "-u", help="Usuário."),
    password: str = typer.Option(
        ..., "--password", prompt=True, hide_input=True, help="Senha."
    ),
    schema_name: str = typer.Option(
        None, "--schema", help="Schema padrão (default: user)."
    ),
    timeout: int = typer.Option(
        180, "--timeout", "-t", help="Timeout em segundos para operações no banco (default: 180)."
    ),
) -> None:
    """Adiciona um profile de conexão Oracle."""
    from sqlmentor.connector import add_connection

    add_connection(
        name=name,
        host=host,
        port=port,
        service=service,
        user=user,
        password=password,
        schema=schema_name,
        timeout=timeout,
    )
    console.print(f"[green]✓[/green] Conexão [bold]{name}[/bold] salva.")

    # Valida conexão e detecta versão/modo automaticamente
    console.print(f"[cyan]Validando conexão...[/cyan]")
    try:
        from sqlmentor.connector import diagnose_connection
        info = diagnose_connection(name)
        console.print(f"[green]✓ Conectado![/green]")
        console.print(f"  Versão: {info['version']}")
        console.print(f"  Schema: {info['schema']}")
        console.print(f"  Modo: [bold]{info['mode']}[/bold]")

        major = int(info["major_version"])
        if major > 0 and major < 12:
            console.print(
                f"  [yellow]⚠ Oracle {major} detectado — requer thick mode (Oracle Instant Client).[/yellow]"
            )
            if info["mode"] == "thick":
                console.print(f"  [green]✓ Thick mode ativo — tudo certo.[/green]")
            else:
                console.print(
                    f"  [red]✗ Thick mode não disponível. Instale o Oracle Instant Client:[/red]\n"
                    f"    https://www.oracle.com/database/technologies/instant-client.html\n"
                    f"    Após instalar, adicione ao PATH e re-teste com: sqlmentor config test -n {name}"
                )
    except RuntimeError as e:
        # _init_thick_mode_if_available levantou RuntimeError — banco antigo sem Instant Client
        console.print(f"[yellow]⚠ Conexão salva, mas validação falhou:[/yellow] {e}")
    except Exception as e:
        console.print(f"[yellow]⚠ Conexão salva, mas validação falhou:[/yellow] {e}")
        console.print(f"  Verifique host/porta/service/credenciais e re-teste com: sqlmentor config test -n {name}")


@config_app.command("list")
def config_list() -> None:
    """Lista conexões salvas."""
    from sqlmentor.connector import get_default_connection, list_connections

    connections = list_connections()
    if not connections:
        console.print("[yellow]Nenhuma conexão configurada.[/yellow]")
        console.print("Use: sqlmentor config add --name <nome> --host <host> --service <service> --user <user>")
        return

    default_name = get_default_connection()

    table = Table(title="Conexões", show_header=True)
    table.add_column("Nome", style="bold cyan")
    table.add_column("Host")
    table.add_column("Porta")
    table.add_column("Service")
    table.add_column("User")
    table.add_column("Schema")
    table.add_column("Timeout")
    table.add_column("Default", justify="center")

    for name, cfg in connections.items():
        is_default = "★" if name == default_name else ""
        table.add_row(
            name,
            cfg.get("host", "?"),
            str(cfg.get("port", "?")),
            cfg.get("service", "?"),
            cfg.get("user", "?"),
            cfg.get("schema", "?"),
            f"{cfg.get('timeout', 180)}s",
            is_default,
        )

    console.print(table)


@config_app.command("set-default")
def config_set_default(
    name: str = typer.Option(..., "--name", "-n", help="Nome do profile a definir como padrão."),
) -> None:
    """Define uma conexão como padrão para analyze/inspect/parse."""
    from sqlmentor.connector import set_default_connection

    try:
        set_default_connection(name)
        console.print(f"[green]✓[/green] Conexão [bold]{name}[/bold] definida como padrão.")
    except ValueError as e:
        console.print(f"[red]Erro:[/red] {e}")
        raise typer.Exit(1)


@config_app.command("test")
def config_test(
    name: str = typer.Option(..., "--name", "-n", help="Nome do profile."),
) -> None:
    """Testa uma conexão Oracle."""
    from sqlmentor.connector import test_connection

    console.print(f"[cyan]Testando:[/cyan] {name}...")
    try:
        info = test_connection(name)
        console.print(f"[green]✓ Conectado![/green]")
        console.print(f"  Versão: {info['version']}")
        console.print(f"  Schema: {info['schema']}")
    except Exception as e:
        console.print(f"[red]✗ Falha:[/red] {e}")
        raise typer.Exit(1)


@config_app.command("remove")
def config_remove(
    name: str = typer.Option(..., "--name", "-n", help="Nome do profile."),
) -> None:
    """Remove uma conexão."""
    from sqlmentor.connector import remove_connection

    if remove_connection(name):
        console.print(f"[green]✓[/green] Conexão [bold]{name}[/bold] removida.")
    else:
        console.print(f"[yellow]Conexão '{name}' não encontrada.[/yellow]")


# ═══════════════════════════════════════════════════════════════════
# DOCTOR
# ═══════════════════════════════════════════════════════════════════


@app.command()
def doctor() -> None:
    """Diagnóstico do ambiente: Python, oracledb, Instant Client, conexões."""
    import platform
    import importlib.metadata

    console.print(Panel.fit("[bold]sqlmentor doctor[/bold]", border_style="cyan"))

    # Python
    py_ver = platform.python_version()
    console.print(f"  Python: [bold]{py_ver}[/bold]")

    # oracledb
    try:
        oradb_ver = importlib.metadata.version("oracledb")
        console.print(f"  oracledb: [bold]{oradb_ver}[/bold]")
    except importlib.metadata.PackageNotFoundError:
        console.print("  oracledb: [red]não instalado[/red]")
        return

    # sqlmentor
    from sqlmentor import __version__
    console.print(f"  sqlmentor: [bold]{__version__}[/bold]")

    # Oracle Instant Client
    console.print("")
    console.print("[cyan]Oracle Instant Client:[/cyan]")
    from sqlmentor.connector import check_thick_mode_available
    thick_info = check_thick_mode_available()
    if thick_info["available"] == "True":
        console.print(f"  [green]✓ Disponível[/green] — {thick_info['detail']}")
    else:
        console.print(f"  [yellow]✗ Não encontrado[/yellow]")
        console.print(f"    Necessário apenas para Oracle < 12c (modo thick).")
        console.print(f"    Download: https://www.oracle.com/database/technologies/instant-client.html")

    # Conexões
    console.print("")
    console.print("[cyan]Conexões:[/cyan]")
    from sqlmentor.connector import list_connections, diagnose_connection
    connections = list_connections()
    if not connections:
        console.print("  [yellow]Nenhuma conexão configurada.[/yellow]")
        return

    for name, cfg in connections.items():
        console.print(f"  [bold]{name}[/bold] ({cfg.get('host', '?')}:{cfg.get('port', '?')}/{cfg.get('service', '?')})")
        try:
            info = diagnose_connection(name)
            major = int(info["major_version"])
            mode_color = "green" if info["mode"] == "thin" or (info["mode"] == "thick" and major < 12) else "green"
            console.print(f"    [green]✓ Conectado[/green] — {info['version']}")
            console.print(f"    Schema: {info['schema']}  Modo: [{mode_color}]{info['mode']}[/{mode_color}]")
            if major > 0 and major < 12 and info["mode"] == "thick":
                console.print(f"    [yellow]Oracle {major} — thick mode ativo (OK)[/yellow]")
            elif major > 0 and major < 12 and info["mode"] == "thin":
                console.print(f"    [red]Oracle {major} — precisa de thick mode mas Instant Client não encontrado[/red]")
        except RuntimeError as e:
            console.print(f"    [red]✗ {e}[/red]")
        except Exception as e:
            console.print(f"    [red]✗ Falha: {e}[/red]")


# ═══════════════════════════════════════════════════════════════════
# PARSE (modo offline, sem conexão)
# ═══════════════════════════════════════════════════════════════════


@app.command()
def parse(
    sql_file: Path = typer.Argument(
        None,
        help="Arquivo .sql para parse.",
    ),
    sql: str = typer.Option(
        None, "--sql", help="SQL inline (alternativa ao arquivo)."
    ),
    schema: str = typer.Option(
        None, "--schema", "-s", help="Schema padrão."
    ),
    normalized: bool = typer.Option(
        False, "--normalized", "-n", help="SQL normalizado (Datadog, OEM, etc.) — substitui '?' por literais dummy antes do parse."
    ),
    denorm_mode: str = typer.Option(
        "literal", "--denorm-mode", help="Estratégia de desnormalização: 'literal' ('?' → '1') ou 'bind' ('?' → :dn1, :dn2...)."
    ),
) -> None:
    """Parse offline — mostra tabelas e colunas sem conectar no banco."""
    from sqlmentor.parser import denormalize_sql, is_normalized_sql, parse_sql

    sql_text, source_label = _resolve_sql_input(sql_file, sql)

    # Auto-detecção de SQL normalizado
    if not normalized and is_normalized_sql(sql_text):
        normalized = True
        console.print("[yellow]⚠ SQL normalizado detectado[/yellow] (placeholders '?' de Datadog/OEM). Desnormalizando automaticamente.")
    if normalized:
        sql_text, _ = denormalize_sql(sql_text, mode=denorm_mode)

    parsed = parse_sql(sql_text, default_schema=schema)

    console.print(Panel.fit(
        f"[bold]Tipo:[/bold] {parsed.sql_type}\n"
        f"[bold]Tabelas:[/bold] {', '.join(parsed.table_names) or 'nenhuma'}\n"
        f"[bold]WHERE cols:[/bold] {', '.join(parsed.where_columns) or 'nenhuma'}\n"
        f"[bold]JOIN cols:[/bold] {', '.join(parsed.join_columns) or 'nenhuma'}\n"
        f"[bold]ORDER BY cols:[/bold] {', '.join(parsed.order_columns) or 'nenhuma'}\n"
        f"[bold]GROUP BY cols:[/bold] {', '.join(parsed.group_columns) or 'nenhuma'}\n"
        f"[bold]Subqueries:[/bold] {parsed.subqueries}\n"
        f"[bold]Parseable:[/bold] {'✓' if parsed.is_parseable else '✗'}",
        title=f"Parse: {source_label}",
        border_style="cyan",
    ))

    if parsed.parse_errors:
        for err in parsed.parse_errors:
            console.print(f"  [yellow]⚠ {err}[/yellow]")


if __name__ == "__main__":
    app()
