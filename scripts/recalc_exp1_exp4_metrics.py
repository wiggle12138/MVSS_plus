#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从 Exp1 / Exp4 raw CSV 重算 metrics JSON（不跑仿真）。"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from metrics_definitions import analyze_run, run_metrics_to_dict  # noqa: E402

EXP1_RAW = ROOT / "results/exp1_scaling/raw"
EXP1_METRICS = ROOT / "results/exp1_scaling/metrics"
EXP1_FILE_RE = re.compile(
    r"^shards(?P<shards>\d+)_nodes(?P<nodes>\d+)_speed(?P<speed>\d+)_inject(?P<inject>\d+)_(?P<strategy>.+)_run(?P<run>\d+)\.json$"
)

EXP4_RAW = ROOT / "results/exp4_eth_workload/raw/dataset_eth_head150k/shards8_nodes4"
EXP4_METRICS = ROOT / "results/exp4_eth_workload/metrics"
EXP4_TOKEN = "eth_head150k"


def _has_run_csv(run_dir: Path) -> bool:
    return (run_dir / "S0_block.csv").exists()


def recalc_exp1() -> int:
    count = 0
    if not EXP1_RAW.exists():
        print(f"[skip] missing {EXP1_RAW}")
        return 0
    EXP1_METRICS.mkdir(parents=True, exist_ok=True)
    for shard_dir in sorted(EXP1_RAW.glob("shards*_nodes*")):
        m_shard = re.match(r"shards(\d+)_nodes(\d+)", shard_dir.name)
        if not m_shard:
            continue
        shards_n, nodes_n = m_shard.group(1), m_shard.group(2)
        for speed_dir in sorted(shard_dir.glob("speed_*")):
            speed = speed_dir.name.replace("speed_", "")
            for maxinj_dir in sorted(speed_dir.glob("maxinj_*")):
                inject = maxinj_dir.name.replace("maxinj_", "")
                for strat_dir in sorted(maxinj_dir.glob("strategy_*")):
                    strategy = strat_dir.name.replace("strategy_", "")
                    for run_dir in sorted(strat_dir.glob("run*")):
                        if not _has_run_csv(run_dir):
                            continue
                        run_n = run_dir.name.replace("run", "")
                        out_name = (
                            f"shards{shards_n}_nodes{nodes_n}_speed{speed}_inject{inject}_"
                            f"{strategy}_run{run_n}.json"
                        )
                        out_path = EXP1_METRICS / out_name
                        try:
                            data = run_metrics_to_dict(analyze_run(run_dir))
                        except Exception as e:
                            print(f"[ERR] {run_dir}: {e}")
                            continue
                        out_path.write_text(
                            json.dumps(data, ensure_ascii=False, indent=2),
                            encoding="utf-8",
                        )
                        count += 1
                        print(
                            f"[exp1] {out_name} rdt={data.get('rdt_ratio', 0):.6f} "
                            f"({data.get('rdt_disordered_count', 0)}/{data.get('rdt_sample_count', 0)})"
                        )
    return count


def recalc_exp4() -> int:
    count = 0
    if not EXP4_RAW.exists():
        print(f"[skip] missing {EXP4_RAW}")
        return 0
    EXP4_METRICS.mkdir(parents=True, exist_ok=True)
    for strat_dir in sorted(EXP4_RAW.glob("strategy_*")):
        strategy = strat_dir.name.replace("strategy_", "")
        for run_dir in sorted(strat_dir.glob("run*")):
            if not _has_run_csv(run_dir):
                continue
            run_n = run_dir.name.replace("run", "")
            out_name = (
                f"dataset_{EXP4_TOKEN}_shards8_nodes4_{strategy}_run{run_n}.json"
            )
            out_path = EXP4_METRICS / out_name
            try:
                data = run_metrics_to_dict(analyze_run(run_dir))
            except Exception as e:
                print(f"[ERR] {run_dir}: {e}")
                continue
            out_path.write_text(
                json.dumps(data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            count += 1
            print(
                f"[exp4] {out_name} rdt={data.get('rdt_ratio', 0):.6f} "
                f"({data.get('rdt_disordered_count', 0)}/{data.get('rdt_sample_count', 0)})"
            )
    return count


def main() -> None:
    n1 = recalc_exp1()
    n4 = recalc_exp4()
    print(f"\n[done] recalculated exp1={n1} exp4={n4} runs")


if __name__ == "__main__":
    main()
