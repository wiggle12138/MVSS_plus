#!/usr/bin/env python3
"""从 BlockTransaction CSV 截取前 N 行（含表头），用于 Exp4 快速加载。"""
from __future__ import annotations

import argparse
import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    p = argparse.ArgumentParser(description="Slice BlockTransaction CSV (header + first N data rows).")
    p.add_argument(
        "--input",
        type=Path,
        default=ROOT / "13000000to13249999_BlockTransaction.csv",
    )
    p.add_argument(
        "--output",
        type=Path,
        default=ROOT / "data/exp4/13000000to13249999_BlockTransaction_head150k.csv",
    )
    p.add_argument("--max-rows", type=int, default=150000, help="Data rows to keep (excl. header).")
    args = p.parse_args()

    if not args.input.exists():
        raise SystemExit(f"Input not found: {args.input}")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    kept = 0
    with args.input.open(encoding="utf-8-sig", newline="") as fin, args.output.open(
        "w", encoding="utf-8", newline=""
    ) as fout:
        reader = csv.reader(fin)
        writer = csv.writer(fout)
        header = next(reader)
        writer.writerow(header)
        for row in reader:
            if kept >= args.max_rows:
                break
            writer.writerow(row)
            kept += 1

    size_mb = args.output.stat().st_size / (1024 * 1024)
    print(f"[OK] rows={kept} out={args.output} size={size_mb:.1f}MB")


if __name__ == "__main__":
    main()
