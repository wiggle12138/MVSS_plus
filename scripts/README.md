# scripts/ 索引

## 实验驱动（PowerShell，由根目录 .bat 调用）

| 脚本 | 用途 |
|------|------|
| `run_exp1_scaling.ps1` | Exp1：分片 × 速率 × 策略矩阵（关探针） |
| `run_probe_matrix.ps1` | Exp2/Exp6 共用：开探针、窗口扫参 |

根目录入口：`run_exp1.bat`、`run_exp2.bat`、`run_exp4.bat`、`run_exp6.bat`。

| 脚本 | 用途 |
|------|------|
| `run_exp4_eth.ps1` | Exp4：BlockTransaction 四策略批跑（关探针） |

## 指标与分析

| 脚本 | 用途 |
|------|------|
| `metrics_definitions.py` | 单次 run 指标计算（与 `说明文档/实验指标定义.md` 对应） |
| `analyze_sync_probe.py` | Sync 探针日志分析 |
| `analyze_logs.py` | 通用日志分析 |
| `summarize_exp1_grid.py` | Exp1 网格汇总（`--latest-per-combo` → `exp1_latest_summary.*`） |
| `summarize_exp2_metrics.py` | Exp2 汇总 |
| `summarize_exp4_metrics.py` | Exp4 汇总 |
| `recalc_exp6_metrics.py` | Exp6 从 raw 重算 metrics（2×2 + 4×4） |

## 出图

| 目录 | 用途 |
|------|------|
| `plots/exp1/plot_exp1.py` | Exp1 可扩展性图 |
| `plots/exp2/plot_exp2.py` | Exp2 对比图 |
| `plots/exp4/plot_exp4.py` | Exp4 四策略对比图 |
| `plots/exp6/plot_exp6.py` | Exp6 窗口敏感性图 |`$env:PYTHONNOUSERSITE='1'`（避免 numpy/matplotlib 与用户 site-packages 冲突）。

## 结果目录约定

```text
results/
├── exp1_scaling/{raw,metrics,summary}
├── exp2_concurrency/{raw,metrics,summary}
├── exp4_eth_workload/{raw,metrics,summary}
├── exp6_sensitivity/{raw,metrics,summary,metrics_recalc_v2}   # 2×2
├── exp6_scale_4x4_full/raw                                     # 4×4 主结论
└── figures/{exp1,exp2,exp4,exp6}/
```
