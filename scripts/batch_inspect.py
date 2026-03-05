"""
Batch inspect de SQL candidates — gera relatórios em 3 verbosidades.

Lê sql/sql_candidates.csv, extrai sql_ids únicos e roda
`sqlmentor inspect` para cada um nos modos full, compact e minimal.

Uso:
    python scripts/batch_inspect.py
    python scripts/batch_inspect.py --conn prod --csv sql/sql_candidates.csv
    python scripts/batch_inspect.py --dry-run          # mostra o que faria sem executar
"""

import argparse
import csv
import subprocess
import sys
import time
from pathlib import Path


def parse_csv(csv_path: str) -> list[str]:
    """Lê o CSV e retorna lista de sql_ids únicos preservando ordem."""
    seen: set[str] = set()
    sql_ids: list[str] = []

    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sql_id = row["SQL_ID"].strip().strip('"')
            if sql_id and sql_id not in seen:
                seen.add(sql_id)
                sql_ids.append(sql_id)

    return sql_ids


def run_inspect(
    sql_id: str,
    conn: str,
    verbosity: str,
    output_dir: Path,
) -> tuple[bool, str]:
    """Executa sqlmentor inspect e salva o relatório."""
    output_file = output_dir / f"{sql_id}_{verbosity}.md"

    cmd = [
        "sqlmentor", "inspect", sql_id,
        "--conn", conn,
        "--verbosity", verbosity,
        "--output", str(output_file),
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,
        )
        if result.returncode == 0:
            return True, str(output_file)
        return False, result.stderr.strip() or result.stdout.strip()
    except subprocess.TimeoutExpired:
        return False, "timeout (300s)"
    except FileNotFoundError:
        return False, "sqlmentor não encontrado no PATH"


def main():
    parser = argparse.ArgumentParser(description="Batch inspect de SQL candidates")
    parser.add_argument("--csv", default="sql/sql_candidates.csv", help="Caminho do CSV")
    parser.add_argument("--conn", default="prod", help="Profile de conexão (default: prod)")
    parser.add_argument("--output-dir", default="reports/batch", help="Diretório de saída")
    parser.add_argument("--dry-run", action="store_true", help="Mostra comandos sem executar")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"Erro: CSV não encontrado: {csv_path}")
        sys.exit(1)

    sql_ids = parse_csv(str(csv_path))
    print(f"SQL IDs únicos: {len(sql_ids)}")

    verbosities = ["full", "compact", "minimal"]
    total = len(sql_ids) * len(verbosities)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.dry_run:
        for sql_id in sql_ids:
            for v in verbosities:
                out = output_dir / f"{sql_id}_{v}.md"
                print(f"  sqlmentor inspect {sql_id} --conn {args.conn} --verbosity {v} --output {out}")
        print(f"\nTotal: {total} execuções ({len(sql_ids)} sql_ids x {len(verbosities)} verbosities)")
        return

    ok = 0
    fail = 0
    start = time.perf_counter()

    for i, sql_id in enumerate(sql_ids, 1):
        for v in verbosities:
            label = f"[{ok + fail + 1}/{total}] {sql_id} ({v})"
            print(f"{label} ...", end=" ", flush=True)

            success, detail = run_inspect(sql_id, args.conn, v, output_dir)

            if success:
                ok += 1
                print(f"OK -> {detail}")
            else:
                fail += 1
                print(f"FALHOU: {detail}")

    elapsed = time.perf_counter() - start
    print(f"\nConcluído: {ok} OK, {fail} falhas, {elapsed:.1f}s total")


if __name__ == "__main__":
    main()
