# Exp6：MVSS-Delta 增量聚合窗口敏感性

## 两种「窗口」（勿混）

| | 论文：OrderList 滑动窗口 **W** | **本实验**：增量聚合窗口 |
|--|-------------------------------|-------------------------|
| **作用** | 多看几笔逻辑序交易 → 减少时间交错**漏判**、**早发现** | 多条 delta **等多久**再合并为一条 sync 发出 |
| **单位** | **笔数**（1,2,3,5,10…） | **毫秒**（`DeltaAggregateWindowMs`） |
| **代码** | 未完整实现 | ✅ `pbft/mvss_delta.go`；`0`=块末 batch，`>0`=定时 flush |

**Exp6 只扫第二种。** 指标定义见 [实验指标定义.md](../实验指标定义.md) §4.1。

---

## Question / Hypothesis

- **问**：`DeltaAggregateWindowMs` 对 Stage3 时延、sync 次数/带宽、正确性有何影响？
- **预期**：全局 TPS/时延变化小；Stage3 可能随窗口略变；正确性不应被破坏（4×4 下 W=0 可能不稳）。

## Design

| 项 | 取值 |
|----|------|
| 策略 | MVSS-Delta |
| 变量 | `DeltaAggregateWindowMs ∈ {0,50,100,200,500}` |
| 控制 | probe=**50**，inject=**24000**，`EnableSyncProbe=true`，`selectedTxs_300K.csv` |
| 规模 | 2×2 + 4×4，各 run=3 |
| **主读数** | `exp6_stage3_makespan_ms`、`sync_send_count`、`sync_batch_size_mean`、`probe_ok` |
| **辅读数** | `tps_global`、`latency_p95`（对窗口不敏感，作无退化旁证） |

**机制要点**：该参数**只改 delta flush 时机**，不改 delta 内容；`window=0` 时同块已在块末 `batch=N` 合并。

**验收**：迁移三日志 + sync 闭环 + `abort,delta=0` + `probe_ok`。

## 运行

**2×2 参考规模**（默认 `OUT_ROOT=results/exp6_sensitivity/raw`）：

```bat
set MAX_INJECT_TXS=24000
set SYNC_PROBE_MAX_ACCOUNTS=50
set NODE_WAIT_SEC=12

run_exp6.bat
```

**4×4 主结论**（与 Exp2 Delta 臂同源）：

```bat
set OUT_ROOT=results/exp6_scale_4x4_full/raw
run_exp6.bat selectedTxs_300K.csv MVSS-Delta "4" 4 "0,50,100,200,500" 3
```

单窗口：`... MVSS-Delta "4" 4 "200" 3`。bat 参数：`数据集 策略 "分片" 节点数 "窗口ms" run数`（分片/窗口**带引号**）。

重算指标：`python scripts/recalc_exp6_metrics.py` → `results/exp6_sensitivity/metrics_recalc_v2/`。

## Result（3 run 均值，probe=50，inject=24000）

> 来源：`metrics_recalc_v2/exp6_recalc_summary.md`，未重跑仿真。

**结论**：全局 TPS/时延跨窗口 **<0.1%**；4×4 下 `sync_send`≈3、`batch_mean`≈17 **不变**；Stage3 随 W↑略降（~400 ms）；4×4 **W=0 probe_ok 33%**，**W≥50 全过**。

### 4×4（选窗依据）

| W(ms) | tps_global | stage3_ms | sync_send | batch_mean | probe_ok |
|-------|------------|-----------|-----------|------------|----------|
| 0 | 306.84 | 4083 | 3 | 16.9 | **33%** |
| 100 | 306.85 | 3968 | 3 | 16.7 | 100% |
| **200** | **306.91** | **3877** | 3 | 16.7 | **100%** |
| 500 | 306.89 | 3597 | 3 | 16.7 | 100% |

**Exp2 固定 Delta 窗口 = 200 ms**（正确性稳定，Stage3 优于 0，比 500 少人为等待）。详见 Exp2 文档。

### 2×2（参考，噪声大）

| W | stage3_ms | probe_ok |
|---|-----------|----------|
| 0 / 500 | ~7200 / ~8050 | 100% |
| 100 / 200 | ~3800 / ~3720 | 100% |

## 限制

- 当前负载下 batch 已块边界饱和 → 扫 ms 窗口对 send 条数影响有限。
- 不等于论文完整 Exp6（尚缺 OrderList **W** 扫描、50% 交错负载等）。

## 产物（canonical，勿与中间目录混淆）

**正式 Exp6 只认以下两处 raw + 一处汇总**（与 `scripts/recalc_exp6_metrics.py` 一致）：

| 规模 | 原始 CSV/日志 | 说明 |
|------|---------------|------|
| **2×2** | `results/exp6_sensitivity/raw/shards2_nodes2/window_{W}/run{K}/` | 参考规模，噪声较大 |
| **4×4** | `results/exp6_scale_4x4_full/raw/shards4_nodes4/window_{W}/run{K}/` | **主结论与 Exp2 Delta 臂数据源** |
| 汇总 | `results/exp6_sensitivity/metrics_recalc_v2/` | 重算 JSON + `exp6_recalc_summary.md` |

单 run 指标（可选）：`results/exp6_sensitivity/metrics/*_probe50_inject24000.json`

**勿保留/勿引用**：`results/exp6_scale_4x4/`（仅为扫窗前的 W=200 中间批，与 `_full` 重复）、`probe=3` 冒烟目录、`raw_pool` 内矩阵脚本自动复制的重复副本（除非作为跨实验统一归档 intentionally 写入）。
