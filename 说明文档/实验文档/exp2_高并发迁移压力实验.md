# Exp2：高并发迁移压力（MVSS vs MVSS-Delta）

## Question / Hypothesis

- **问**：probe 并发上升时，Delta 相对 MVSS 能否显著降低 sync 开销且保持正确性？
- **预期**：Delta 的 `sync_send_count` / `sync_bandwidth_mb` 更低；全局 TPS/时延差异小；`probe_ok=100%`。

## Design

| 项 | 取值 |
|----|------|
| 策略 | MVSS vs MVSS-Delta |
| 变量 | `SyncProbeMaxAccounts`（正式 **50**；可扩 20/50 阶梯） |
| 控制 | **4×4**，inject=**24000**，probe 开，`selectedTxs_300K.csv` |
| Delta 窗口 | **200 ms**（Exp6 收敛，[exp6 文档](./exp6_窗口敏感性实验.md)） |
| MVSS 窗口 | `0`（策略不用） |
| run | 3 |

**探针 vs 迁出总数**：`SyncProbeMaxAccounts` 是从 PageRank `newAddrs` 里最多取 N 个做 Stage3 探针；实际数 = `min(N, len(newAddrs))`，不会报错。

**主读数**：`sync_bandwidth_mb`、`sync_send_count`、`exp6_stage3_makespan_ms`、`probe_ok`。  
**辅读数**：`tps_global`、`latency_p95`。

**验收**：sync 闭环 + `abort,delta=0` + `probe_ok`。

## 运行

**Delta 臂**：复用 Exp6，**不必重跑**  
`results/exp6_scale_4x4_full/raw/shards4_nodes4/window_200/run{1,2,3}/`

**MVSS 臂**（补跑）：

```bat
set MAX_INJECT_TXS=24000
set SYNC_PROBE_MAX_ACCOUNTS=50
set NODE_WAIT_SEC=12

run_exp2.bat
```

汇总：`python scripts/summarize_exp2_metrics.py --probe 50`  
→ `results/exp2_concurrency/summary/exp2_probe50_metrics.md`

**bat 参数**（Exp6 单策略扫窗）：`run_exp6.bat 数据集 策略 "分片" 节点数 "窗口ms" run数`。Exp2 一键入口：`run_exp2.bat [数据集] [probe数] [run数] [Delta窗口ms]`。

## Result（4×4，probe=50，run=3）

| 策略 | tps_global | latency_p95 | sync_send | 带宽 MB | stage3_ms | probe_ok |
|------|------------|-------------|-----------|---------|-----------|----------|
| MVSS | 306.9±0.05 | 9784±5 | 100 | 0.193 | 4076±2 | 3/3 |
| MVSS-Delta | 306.9±0.03 | 9791±6 | **3** | **0.012** | **3877±3** | 3/3 |

- Delta 带宽约 **16×** 低于 MVSS，Stage3 约快 **200 ms**；全局 TPS/时延无显著差异。
- RDT=0，正确性全过。

## 限制

- 并发由探针账户数代理，非真实热迁移规模；结论限于当前 4×4 工程实现。

## 产物

```text
results/exp2_concurrency/raw/probe_50/strategy_MVSS/shards4_nodes4/window_0/run{K}/
results/exp2_concurrency/metrics/MVSS_probe50_run{K}.json
results/exp2_concurrency/summary/exp2_probe50_metrics.md
```
