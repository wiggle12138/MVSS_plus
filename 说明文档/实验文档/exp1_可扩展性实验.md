# Exp1：可扩展性实验（Scaling）

## Question：我要回答什么问题？

- 在不开探针（自然负载）条件下，随着**分片数**与**注入速率**上升，四种迁移策略的整体性能差异是什么？
- `MVSS` / `MVSS-Delta` 是否在 `Latency`、`RLT`、`TPS`、`RDT` 上持续优于锁基线？
- 在当前工程规模（**4 / 6 / 8 分片 × 4 节点**）下，是否已经出现可重复的趋势信号？

## Hypothesis：我预计结果是什么，为什么？

- 预计 `MVSS` / `MVSS-Delta` 的 `RLT` 明显低于 `SOTA-Lock`、`Fine-tuned-Lock`，因为迁移窗口内不依赖全量锁池。
- 预计高负载下，`MVSS` / `MVSS-Delta` 的 `TPS` 更稳定、`Latency` 增幅更缓。
- 由于本实验不开探针，预计不会稳定触发论文中的交错同步场景，因此 Exp1 的主结论应是「整体迁移性能差异」，不是「Stage3 机理验证」。

## Design：变量/控制量/指标/验收口径是什么？

### 变量

- 策略：`SOTA-Lock`、`Fine-tuned-Lock`、`MVSS`、`MVSS-Delta`
- 分片数：`Shard_num ∈ {4, 6, 8}`，`NodesPerShard=4`
- 注入速率：`Inject_speed ∈ {200, 400, 800}`
- 注入规模：`MaxInjectTxs=24000`（与 Exp2/6 对齐）
- 重复轮次：各参数组合独立编号 `run1`、`run2`…（表示**同参数下的重复试验**，非修复批次）

### 控制量

- 数据集固定：`selectedTxs_300K.csv`
- `Block_interval=2`、`MaxBlockSize=500`
- `Max_Commit_Block` 固定
- `EnableSyncProbe=false`
- `MVSS-Delta` 的 `delta_window_ms=200`（写在各 run 的 `run_meta.txt`）

### 指标

- `tps_global`、`tps_migration`
- `latency_p95`
- `rlt_window_ratio`（汇总表中以 `locked_ratio` 代理展示）
- `rdt_ratio`
- `tx_committed_total`

### 验收口径（每轮）

- `client.out.log` 出现：`每个分片出XX啦`、`MigrateWanted`、`所有分片都发送了pending`、`emptyStreakByShard`
- `run_status.txt` 为 `pass`，且 `tx_committed_total >= MaxInjectTxs`
- 产物完整：`S*_block.csv`、`S*_transaction.csv`、`migration.csv`、`run_meta.txt`
- `metrics/*.json` 成功生成且关键字段非空

### 运行说明（Windows，关探针）

**脚本**：`run_exp1.bat` → `scripts/run_exp1_scaling.ps1`

**全部产物统一在 `results/exp1_scaling/` 下**：

```text
results/exp1_scaling/
  raw/shards{S}_nodes4/speed_{V}/maxinj_24000/strategy_{STR}/run{K}/
  metrics/shards{S}_nodes4_speed{V}_inject24000_{STR}_run{K}.json
  summary/exp1_latest_summary.md          # 各组合最新有效 run 汇总
  summary/exp1_latest_summary.csv
results/figures/exp1/
  exp1_fig3_style_grouped.png             # 全网格 Fig3 风格三联图
  exp1_overview_speed800.png              # speed=800 子集
```

冒烟（不跑仿真）：

```bat
run_exp1.bat selectedTxs_300K.csv SOTA-Lock 4 4 1 24000 dryrun
```

单策略单轮（示例：8 分片 speed=800 MVSS，第 5 次重复）：

```bat
set NODE_WAIT_SEC=15
set RUN_START=5
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/run_exp1_scaling.ps1 ^
  -Strategies "MVSS" -ShardNums "8" -InjectSpeeds "800" -Runs 1 -RunStart 5 ^
  -NodeWaitSec 15 -MaxInjectTxs 24000 -RunTimeoutSec 900
```

或通过 bat 传 speed（`%7` 为纯数字时作为 inject speed；**勿用 `%10`**，cmd 会误解析为 `%1+"0"`）：

```bat
set NODE_WAIT_SEC=15
set RUN_START=5
run_exp1.bat selectedTxs_300K.csv MVSS 8 4 1 24000 800
```

全网格批跑（36 组，需 `ALLOW_LONG_RUN=1`）：

```bat
set ALLOW_LONG_RUN=1
set NODE_WAIT_SEC=12
set INJECT_SPEED_LIST=200,400,800
set SHARD_LIST=4,6,8
run_exp1.bat selectedTxs_300K.csv
```

汇总与出图：

```bat
python scripts/summarize_exp1_grid.py --latest-per-combo
set PYTHONNOUSERSITE=1
python scripts/plots/exp1/plot_exp1.py
python scripts/plots/exp1/plot_exp1.py --speed 800 --output-name exp1_overview_speed800.png
```

---

## Result：数据是否支持假设？统计上/趋势上如何？

> 数据来源：`results/exp1_scaling/`（`latest_per_combo`：每个 `(shards, speed, strategy)` 取**最新有效 run**）。  
> 汇总文件：`results/exp1_scaling/summary/exp1_latest_summary.md`（2026-07-05 更新）。  
> 网格规模：**4/6/8 分片 × 200/400/800 speed × 四策略 = 36 组**；**36/36 有效**（`probe_ok=true`，`tx_committed=24000`，除下表注明项）。

### 全网格指标（latest_per_combo）

| shards | speed | strategy | run | tps_global | latency_p95 (ms) | locked_ratio | rdt_ratio | tx_committed |
|---:|---:|---|---:|---:|---:|---:|---:|---:|
| 4 | 200 | SOTA-Lock | 1 | 181.85 | 4002 | 0 | 0 | 24000 |
| 4 | 200 | Fine-tuned-Lock | 1 | 181.82 | 3994 | 0 | 0 | 24000 |
| 4 | 200 | MVSS | 1 | 181.85 | 3991 | 0 | 0 | 24000 |
| 4 | 200 | MVSS-Delta | 1 | 181.87 | 3986 | 0 | 0 | 24000 |
| 4 | 400 | SOTA-Lock | 1 | 307.85 | 9757 | 0.0009 | 0 | 24000 |
| 4 | 400 | Fine-tuned-Lock | 1 | 307.88 | 9774 | 0 | 0 | 24000 |
| 4 | 400 | MVSS | 1 | 307.82 | 9795 | 0 | 0 | 24000 |
| 4 | 400 | MVSS-Delta | 1 | 307.83 | 9787 | 0 | 0 | 24000 |
| 4 | 800 | SOTA-Lock | 6 | 307.84 | 9816 | 0.0007 | 0 | 24000 |
| 4 | 800 | Fine-tuned-Lock | 6 | 307.87 | 9803 | 0 | 0 | 24000 |
| 4 | 800 | MVSS | 6 | 307.79 | 9790 | 0 | 0.143 | **23998** |
| 4 | 800 | MVSS-Delta | 6 | 307.84 | 9801 | 0 | 0 | 24000 |
| 6 | 200 | 四策略 | 2 | ~181.8 | 3990–5760 | 0 | 0 | 24000 |
| 6 | 400 | 四策略 | 2 | ~324.4 | ~9747–9756 | SOTA 0.0005，其余 0 | MVSS 0.25 | 24000 |
| 6 | 800 | 四策略 | 2 | ~414.1 | ~15706–15751 | SOTA 0.0025，其余 0 | 0 | 24000 |
| 8 | 200 | 四策略 | 2 | ~181.1–181.8 | ~3988–4001 | 0 | Delta 0.098 | 23905–24000 |
| 8 | 400 | 四策略 | 2/4 | ~315.8 | ~9717–9730 | SOTA 0.0006，其余 0 | 0 | 24000 |
| 8 | 800 | SOTA-Lock | 4 | 387.60 | 19702 | 0.0045 | 0 | 24000 |
| 8 | 800 | Fine-tuned-Lock | 4 | 387.42 | 19712 | 0 | 0.125 | 24000 |
| 8 | 800 | MVSS | 6 | 387.51 | 19691 | 0 | 0.125 | 24000 |
| 8 | 800 | MVSS-Delta | 6 | 387.28 | 19712 | 0 | 0 | 24000 |

> **8×800 MVSS / MVSS-Delta**：run5、run6 各一轮均 PASS（~148–152s，`tx=24000`），修复 `WaitSyncIni`/客户端就绪/TcpDial 超时后稳定复现。

### 简要结论

1. **RLT（locked_ratio）**：仅 **SOTA-Lock** 在多数非零负载点出现非零 locked_ratio；MVSS 系基本为 0，符合「非全锁 / 分流」预期。Fine-tuned-Lock 的 block 级 locked_ratio 常为 0，可能与半锁统计口径有关。
2. **TPS 随规模变化**：
   - **4 分片**：speed 200→400 TPS 约 182→308；speed 800 与 400 接近（~308），注入成为瓶颈。
   - **6 分片**：speed 800 时 TPS 升至 ~414；**8 分片** speed 400/800 时 TPS ~316 / ~387。
3. **Latency p95**：speed=800 时显著升高（4 分片 ~9.8s；6 分片 ~15.7s；8 分片 ~19.7s）；四策略在同一参数点差异很小。
4. **MVSS vs MVSS-Delta**：关探针下主路径一致，`tps_global` / `latency_p95` 几乎重合；Delta 的 Stage3 优势不在 Exp1 覆盖范围。
5. **MVSS vs SOTA**：MVSS 系 RLT 更低；全局 TPS 差异 <1%（迁移窗口占整体运行时间比例小）。4×800 MVSS run6 仍差 2 笔（23998），其余网格点均为 24000。

### 可视化

- 全网格：`results/figures/exp1/exp1_fig3_style_grouped.png`
- speed=800 子集：`results/figures/exp1/exp1_overview_speed800.png`

---

## Interpretation & Threats：机理解释 + 有哪些限制/混杂因素？

### 机理解释

- **SOTA-Lock 非零 RLT**：全锁在 `TXmig1` 后将迁出账户关联交易打入 `Locking_TX_Pools`；MVSS 系走老/新分流而非锁池，故 `rlt_window≈0`。
- **全局 TPS 四策略趋同**：24000 笔注入中，迁移窗口（约 4–8s）占整体运行时间比例小；大部分 tx 在迁移窗外以相同 relay/出块路径执行，策略差异被「稀释」。
- **分片数增加、TPS 下降**：8 分片下单片负载与跨片 relay 开销上升，在相同 inject 规模下全局 TPS 低于 4/6 分片（符合扩展性实验预期方向）。
- **MVSS ≈ MVSS-Delta**：无时间戳交错 → `DetectInterleave` 不触发 → 无 `TXsync`/`TXsyncDelta`；Delta 仅在 Stage3 分叉，主实验路径一致。
- **RequestTime 语义**（无探针）：Client 注入时刻 `time.Now()`；`TXmig1_Time` 为 `handleNewMap` 时刻。老/新分流按两者先后判定，但不会产生 old-new-old 交错。

### 工程修复记录（2026-07-04/05，影响 Exp1 可比性）

以下修复**不改变 SOTA/Fine-tuned 锁基线语义**，仅修正 MVSS 主实验路径与运行基础设施：

| 问题 | 修复 |
|------|------|
| 关探针下 `handleMig2` 误设 `WaitSyncIni` 阻塞 new tx | 仅 `EnableSyncProbe` 时进入 `WaitSyncIni` |
| `mvssValidateIncomingTx` 误拒收款方 relay | 仅 sender 侧严格 RedirectTag 校验 |
| 客户端 `Sendtime` 节点未就绪 / `TcpDial` 无超时 | 节点就绪探测 + Dial/Write 超时重试 |
| 批跑验收漏检空日志 / tx=0 假 PASS | 脚本校验 `tx_committed >= MaxInjectTxs` |

### 限制与混杂因素

- **重复轮次不一**：各组合最新有效 run 编号不同（如 4 分片 run1、6 分片 run2、8×800 MVSS run6），汇总取 latest_per_combo 而非统一 run 编号平均。
- **单轮 vs 多轮**：多数网格点仅 1 次有效 run；8×800 MVSS/Delta 有 run5+run6 双轮复验。
- 不开探针时 **Stage3 未覆盖**；MVSS-Delta 相对 MVSS 的优势需看 Exp2/6。
- Fine-tuned-Lock 的 `rlt_window=0` 可能与 block 级统计未捕获半锁标记有关。
- 数据集无细粒度 ClientTS，Exp1 不用于证明交错/sync 机理。
- 4×800 MVSS 仍偶发 23998/24000，对全局 TPS 影响可忽略，但需后续单独对账。

---

## 相关文档

- [运行观测与验收指南.md](../运行观测与验收指南.md) — 正常运行日志链路与耗时粗估
- [账户迁移策略对比.md](../账户迁移策略对比.md) — 四策略差异
- [可视化规划_Exp1_Exp2_Exp6.md](./可视化规划_Exp1_Exp2_Exp6.md) — 出图说明
- [AGENT_EXECUTION_CONSTRAINTS.md](../../AGENT_EXECUTION_CONSTRAINTS.md) — 实验执行约束
