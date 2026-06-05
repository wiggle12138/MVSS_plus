# MVSS Block Emulator

基于 **Go** 的分片区块链仿真器，实现 **MVSS（多版本状态同步）** 相关的跨分片交易、账户迁移与中继（relay）逻辑。适用于论文复现与性能实验，默认采用简化的单节点 PBFT 出块模型。

![全流程](D:\Desktop\实验室\MVSS+\MVSS-main\说明文档\全流程.png)

## 环境要求

- Go **1.20+**
- Windows 下可直接使用提供的批处理；Linux/macOS 需按下方命令手动启动各进程

## 快速运行（2 分片 × 2 节点）

1. 将交易数据集（如 `selectedTxs_300K.csv`）放在项目根目录。
2. 双击或在命令行执行：

```bat
start_2shard_2node.bat
REM 或指定数据集与迁移策略（MVSS=现有逻辑，MVSS+=新方法接口）：
start_2shard_2node.bat selectedTxs_300K.csv MVSS
start_2shard_2node.bat selectedTxs_300K.csv MVSS-Delta
```

验证 MVSS sync 通路时，client 另加 `--enableSyncProbe`（或 `params/config.go` 中 `EnableSyncProbe=true`），跑完后查看 `log/S*_sync.csv` 与探针交易 Id `9000000001` 起，详见 `说明文档/Sync探针注入.md`；Delta 出站窗口聚合见 `说明文档/聚合窗口.md`。

脚本会依次启动 **S0-N0、S0-N1、S1-N0、S1-N1** 四个节点窗口，等待数秒后启动 **客户端**（监听 `127.0.0.1:8800`）。节点默认监听 `8010/8011/8020/8021`。

首次 `go run` 编译较慢，可将环境变量 `NODE_WAIT_SEC` 调大（如 `60`）再运行批处理。

## 手动启动

**分片节点**（每个分片需至少一个节点，仅 **N0** 负责出块）：

```bash
go run main.go -S 2 -s S0 -f 0 -n N0 -t selectedTxs_300K.csv
go run main.go -S 2 -s S0 -f 0 -n N1 -t selectedTxs_300K.csv
go run main.go -S 2 -s S1 -f 0 -n N0 -t selectedTxs_300K.csv
go run main.go -S 2 -s S1 -f 0 -n N1 -t selectedTxs_300K.csv
```

**客户端**（注入交易、发送初始时间 LLT）：

```bash
go run main.go -S 2 -f 0 -c -t selectedTxs_300K.csv
# 可选：只注入前 N 笔，便于冒烟测试
go run main.go -S 2 -f 0 -c -t selectedTxs_300K.csv --maxInjectTxs 5000
```

常用参数：`-S` 分片数，`-s` 分片 ID，`-n` 节点 ID，`-f` 恶意节点数（预留），`-t` CSV 路径，`-c` 客户端模式。

客户端可选参数（用于复现实验切片注入）：

- `--maxInjectTxs N`：本次最多注入 N 笔；
- `--injectStartTx K`：从第 K 笔（0-based）开始注入。

例如：`--injectStartTx 20000 --maxInjectTxs 20000` 表示注入区间 `[20000, 40000)`。

## 配置与日志

- 全局参数默认值见 `params/config.go`（出块间隔、块大小、注入速率、迁移策略等）。
- 运行后在 `log/` 下生成各分片的块统计、交易明细、队列长度等 CSV。
- 更详细的参数说明与代码对照见 `说明文档/`（如 `参数配置.md`、`账户迁移策略对比.md`）。

## 复现实验后一键产出图表与表格

当多次实验跑完后，建议先将每次运行的 `log/*.csv` 按策略和轮次整理成如下目录：

```text
results/raw/
├── SOTA-Lock/
│   ├── run1/*.csv
│   ├── run2/*.csv
│   └── ...
└── Fine-Tune-Lock/
    ├── run1/*.csv
    ├── run2/*.csv
    └── ...
```

然后在项目根目录执行：

```bat
run_multi_analysis.bat
```

该脚本会自动串行执行：

1. `scripts/summarize_multi_runs.py`：汇总多次运行指标并输出表格；  
2. `scripts/plot_multi_runs.py`：根据汇总结果生成对比图像。

默认输入目录：`results/raw`  
默认输出目录：`results/multi_run_summary`

可自定义路径：

```bat
run_multi_analysis.bat "你的root目录" "你的输出目录"
```

输出产物包括：

- `run_metrics.csv`
- `strategy_summary.csv`
- `strategy_summary.md`
- `figures/*.png`
- `figures/figures_manifest.md`

## 自动跑 5 轮并归档日志

如果希望按区间注入（每轮 2 万笔）并自动把 `log/*.csv` 归档到 `results/raw/<strategy>/runN/`，可在根目录执行：

```bat
run_5_segments.bat SOTA-Lock selectedTxs_300K.csv
run_5_segments.bat Fine-tuned-Lock selectedTxs_300K.csv
```

默认执行 `run1~run5`，区间分别为：

- run1: `[0, 20000)`
- run2: `[20000, 40000)`
- run3: `[40000, 60000)`
- run4: `[60000, 80000)`
- run5: `[80000, 100000)`

可自定义参数：

```bat
run_5_segments.bat <strategy> <dataset> <runs> <window> <raw_root>
```

## 目录结构

```
.
├── main.go                 # 入口，调用 test.Test_shard()
├── start_2shard_2node.bat  # Windows 一键启动脚本
├── params/                 # 全局配置、分片/节点网络表
├── shard/                  # 分片节点封装（TCP 监听、启动 PBFT）
├── pbft/                   # 共识、客户端注入、跨片消息处理
├── chain/                  # 区块链、出块、状态树更新
├── core/                   # 区块、交易、各类交易池
├── account/                # 账户状态与分片映射
├── storage/                # 本地区块持久化
├── trie/                   # 状态 Merkle Trie
├── algorithm/              # PageRank / METIS 等重分片算法
├── utils/                  # 工具函数（TCP、分片哈希等）
├── test/                   # 命令行解析与节点/客户端启动逻辑
├── log/                    # 实验输出（运行时生成，已 gitignore）
├── record/                 # 状态 trie 磁盘数据（运行时生成）
└── 说明文档/               # 设计说明、参数、调试笔记
```

## 编译

```bash
go build -o blockEmulator.exe .
```

## 说明

- 链数据目录 `*_blockchain_db` 与 `record/triedb/` 为运行时产物，不应纳入版本库。
- 修改 CSV 格式或数据集文件名时，需保证 `test/test_shard.go` 与 `pbft/client.go` 中的解析逻辑一致（如 `selectedTxs_300K.csv` 使用第 4/5 列地址）。
