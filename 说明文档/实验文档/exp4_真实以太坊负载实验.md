# Exp4：真实以太坊负载实验

## Question / Hypothesis

- 在真实链上交易分布回放下，四策略性能与一致性如何？
- 预计 MVSS 系列在 RLT/TPS/RDT 上优于锁基线；**不开探针**，不证 Stage3 交错机理。

RDT 量的是：同一迁出账户的多笔交易，客户端提交顺序和链上实际提交顺序是否一致。

## 数据集

| 文件 | 能否直接用 | 说明 |
|------|------------|------|
| `13000000to13249999_Block_Info.csv` | **否** | 区块级统计，无 from/to |
| `13000000to13249999_BlockTransaction.csv` | **是** | 真实交易明细，与 `0to999999_BlockTransaction.csv` 同格式 |

**列语义**（`dataset_flag=1`）：`from/to` 在列 3/4，`value` 在列 8；自动跳过合约创建（`toCreate!=None`）与合约调用（`fromIsContract/toIsContract=1`）。

**使用注意**：
- 文件约 **11 GB**，client 启动时仅顺序读取**前 100 万行**建账/加载；默认注入 50000 笔在前 100 万行内足够。
- 默认 `Inject_speed=800`（与 Exp1 高负载档对齐）。

Go 识别：文件名后缀 `_BlockTransaction.csv`（已补 `isBlockTransactionDatasetPath`）。

## Design

| 项 | 取值 |
|----|------|
| 策略 | 四策略 |
| 规模 | **8×4**（固定） |
| 注入 | `MaxInjectTxs=50000`，`Inject_speed=800`，探针关 |
| 重复 | 每策略 `run=3` |
| 等待/超时 | `NODE_WAIT_SEC=15`，`RUN_TIMEOUT_SEC=1200` |

## 运行

```bat
set ALLOW_LONG_RUN=1
run_exp4.bat
```

冒烟：`run_exp4.bat dryrun`

## 汇总与出图

```bat
python scripts/summarize_exp4_metrics.py
set PYTHONNOUSERSITE=1
python scripts/plots/exp4/plot_exp4.py
```

## 产物目录

```text
results/exp4_eth_workload/
  raw/dataset_eth_blk13000/shards8_nodes4/strategy_{STR}/run{K}/
  metrics/dataset_eth_blk13000_shards8_nodes4_{STR}_run{K}.json
  summary/exp4_metrics.csv
results/figures/exp4/exp4_strategy_compare.png
```

## Result

- 当前状态：数据集就绪，**待批跑**。

## Interpretation & Threats

- 前 100 万行中约 35% 为可回放普通转账（其余为合约类过滤）；不代表全文件统计。
- 注入速率与链上块间时间戳未逐笔对齐（client 用恒定 `Inject_speed`）。
