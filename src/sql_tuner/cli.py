"""
SQL Tuner CLI — Coleta de contexto para tuning de SQL assistido por IA.

Uso:
    sql-tuner analyze <arquivo.sql> --conn <profile>
    sql-tuner analyze --sql "SELECT ..." --conn <profile>
    sql-tuner parse <arquivo.sql> --schema <SCHEMA>
    sql-tuner parse --sql "SELECT ..." --schema <SCHEMA>
    sql-tuner config add --name prod --host ... --port 1521 --service ORCL --user ...
    sql-tuner config list
    sql-tuner config test --name prod
    sql-tuner config remove --name prod
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
    name="sql-tuner",
    help="Coleta contexto Oracle para tuning de SQL assistido por IA.",
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
        ..., "--conn", "-c", help="Nome do profile de conexão."
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
) -> None:
    """Analisa um SQL e coleta contexto Oracle para tuning."""
    from sql_tuner.collector import collect_context
    from sql_tuner.connector import connect, get_connection_config
    from sql_tuner.parser import parse_sql
    from sql_tuner.report import to_json, to_markdown

    # Resolve fonte do SQL
    sql_text, source_label = _resolve_sql_input(sql_file, sql)

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
        oracle_conn = connect(conn)
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
        remapped: dict[str, str | int | float] = {}
        for sql_name in unique_sql_binds:
            if sql_name.upper() in provided_upper:
                raw_val = provided_upper[sql_name.upper()]
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
            suggested = f"sql-tuner analyze {src_arg} --conn {conn} -x {bind_flags}"
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
) -> None:
    """Adiciona um profile de conexão Oracle."""
    from sql_tuner.connector import add_connection

    add_connection(
        name=name,
        host=host,
        port=port,
        service=service,
        user=user,
        password=password,
        schema=schema_name,
    )
    console.print(f"[green]✓[/green] Conexão [bold]{name}[/bold] salva.")


@config_app.command("list")
def config_list() -> None:
    """Lista conexões salvas."""
    from sql_tuner.connector import list_connections

    connections = list_connections()
    if not connections:
        console.print("[yellow]Nenhuma conexão configurada.[/yellow]")
        console.print("Use: sql-tuner config add --name <nome> --host <host> --service <service> --user <user>")
        return

    table = Table(title="Conexões", show_header=True)
    table.add_column("Nome", style="bold cyan")
    table.add_column("Host")
    table.add_column("Porta")
    table.add_column("Service")
    table.add_column("User")
    table.add_column("Schema")

    for name, cfg in connections.items():
        table.add_row(
            name,
            cfg.get("host", "?"),
            str(cfg.get("port", "?")),
            cfg.get("service", "?"),
            cfg.get("user", "?"),
            cfg.get("schema", "?"),
        )

    console.print(table)


@config_app.command("test")
def config_test(
    name: str = typer.Option(..., "--name", "-n", help="Nome do profile."),
) -> None:
    """Testa uma conexão Oracle."""
    from sql_tuner.connector import test_connection

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
    from sql_tuner.connector import remove_connection

    if remove_connection(name):
        console.print(f"[green]✓[/green] Conexão [bold]{name}[/bold] removida.")
    else:
        console.print(f"[yellow]Conexão '{name}' não encontrada.[/yellow]")


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
) -> None:
    """Parse offline — mostra tabelas e colunas sem conectar no banco."""
    from sql_tuner.parser import parse_sql

    sql_text, source_label = _resolve_sql_input(sql_file, sql)
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
