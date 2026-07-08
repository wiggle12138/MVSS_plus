# Exp2 绘图脚本说明

## 脚本

- `plot_exp2.py`

## 数据来源

- 读取目录：`results/exp2_concurrency/metrics/`
- 文件模式（仅读取正式文件）：
  - `MVSS_probe{P}_run{K}.json`
  - `MVSSDelta_probe{P}_run{K}.json`
- 会自动忽略 `tmp_*.json`、`Delta_4x4_probe3_run1.json` 等非正式命名文件。

## 输出

- 默认输出到：`results/figures/exp2/`
- 图文件：
  - `exp2_compare_probe{P}.png`（每个 probe 账户数一个对比图）
  - `exp2_trend_by_probe.png`（若存在多个 probe 账户数）

## 运行

```bash
python scripts/plots/exp2/plot_exp2.py
```

可选参数：

```bash
python scripts/plots/exp2/plot_exp2.py \
  --metrics-dir results/exp2_concurrency/metrics \
  --output-dir results/figures/exp2
```

