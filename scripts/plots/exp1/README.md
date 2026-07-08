# Exp1 绘图脚本说明

## 脚本

- `plot_exp1.py`

## 数据来源

- 读取目录：`results/exp1_scaling/metrics/`
- 文件模式：
  - `shards{S}_nodes{N}_speed{V}_inject{M}_{STR}_run{K}.json`
  - 示例：`shards4_nodes4_speed800_inject24000_MVSS-Delta_run6.json`

## 输出

- 默认输出到：`results/figures/exp1/`
- 图文件：
  - `exp1_fig3_style_grouped.png`（Fig3 风格三联分组柱状图）

该图默认包含 3 个并排子图：

- `(a) Ratio of Locked TXs`（`locked_ratio`）
- `(b) Throughput (TPS)`（`tps_global`）
- `(c) Ratio of Disorderly TXs`（`rdt_ratio`）

横轴分组为 `[InjectSpeed, ShardNum]`，组内按四策略对比。

## 运行

```bash
python scripts/plots/exp1/plot_exp1.py
```

可选参数：

```bash
python scripts/plots/exp1/plot_exp1.py \
  --metrics-dir results/exp1_scaling/metrics \
  --output-dir results/figures/exp1
```

按指定 run 画图（避免混入历史 run）：

```bash
python scripts/plots/exp1/plot_exp1.py \
  --metrics-dir results/exp1_scaling/metrics \
  --output-dir results/figures/exp1 \
  --run 1 \
  --output-name exp1_fig3_style_grouped_run1.png
```

