# 初步可视化规划（Exp1 / Exp2 / Exp6）

## 目标

- 基于现有已跑数据，先产出一版可用于组会/论文草稿的初步图表。
- 先保证“读得清楚、可复现”，后续再迭代图形美化与统计显著性分析。

---

## 目录与脚本组织（每实验一个文件夹）

建议在 `scripts/plots/` 下按实验拆分：

```text
scripts/plots/
  exp1/
    plot_exp1.py
    README.md
  exp2/
    plot_exp2.py
    README.md
  exp6/
    plot_exp6.py
    README.md
```

输出目录建议统一：

```text
results/figures/
  exp1/
  exp2/
  exp6/
```

---

## 图型选择与变量映射

## Exp1（四策略主实验，关探针）

- 自变量：
  - 当前已有：`strategy`（四组）
  - 后续扩展：`Inject_speed`、`Shard_num`
- 因变量：`tps_global`、`tps_migration`、`latency_p95`、`rlt_window_ratio`、`rdt_ratio`
- 推荐图型：
  1. **分组柱状图**（首选）：同一指标对比四策略（易读、适合单点结果）
  2. **折线图**（扩展后）：`x=Inject_speed` 或 `x=Shard_num`，`y=指标`，按策略分线
- 现有数据阶段（speed=800 单轮）：
  - 先做 1 张四联图（四指标各一个子图）或 4 张柱状图。

## Exp2（高并发迁移压力，MVSS vs MVSS-Delta）

- 自变量：`strategy`（MVSS / MVSS-Delta）
- 因变量：`sync_send_count`、`sync_bandwidth_mb`、`exp6_stage3_makespan_ms`、`probe_ok`
- 推荐图型：
  1. **柱状图**：`sync_send_count`、`sync_bandwidth_mb`（核心主图）
  2. **柱状图/点图**：`stage3_makespan_ms`（两策略对比）
  3. **堆叠条形或文本注记**：`probe_ok`（通过率）
- 若后续扩展并发账户梯度（20/50/...）：
  - 改为折线图，`x=probe_accounts`，`y=开销`，按策略分线。

## Exp6（MVSS-Delta 聚合窗口敏感性）

- 自变量：`DeltaAggregateWindowMs`（0/50/100/200/500）
- 因变量：`exp6_stage3_makespan_ms`、`sync_send_count`、`sync_batch_size_mean`、`probe_ok`
- 推荐图型：
  1. **折线图**（首选）：`x=window_ms`，`y=stage3_makespan_ms`
  2. **折线图**：`x=window_ms`，`y=sync_send_count` / `sync_batch_size_mean`
  3. **柱状图**：`probe_ok`（按窗口）

---

## 初版出图优先级（结合当前已有数据）

1. **Exp2**：先画 `sync_send_count` 与 `sync_bandwidth_mb`（最能体现 MVSS-Delta 价值）  
2. **Exp6**：画 `window_ms -> stage3_makespan_ms` 趋势图（体现参数敏感性）  
3. **Exp1**：画四策略总体指标柱状图（给主实验对比基线）

---

## 数据口径注意事项

- Exp1/Exp4（主实验）默认关探针，不用于论证 Stage3 机理。
- Exp2/Exp6 使用探针数据，主看 sync 相关指标，不与 Exp1 的全局指标混为同一图。
- 现阶段多为单轮/少轮结果，图注中应标明“初步结果（preliminary）”。
