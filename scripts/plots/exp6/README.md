# Exp6 绘图脚本说明

## 脚本

- `plot_exp6.py`

## 数据来源

- 默认读取：`results/exp6_sensitivity/metrics/`
- 文件模式（正式）：
  - `shards{S}_nodes{N}_window{W}_run{K}_probe{P}_inject{M}.json`
  - 示例：`shards4_nodes4_window200_run1_probe50_inject24000.json`

## 输出

- 默认输出到：`results/figures/exp6/`
- 图文件（按规模区分）：
  - `exp6_core_shards{S}_nodes{N}.png`
  - `exp6_global_shards{S}_nodes{N}.png`

## 运行

4x4 主规模：

```bash
python scripts/plots/exp6/plot_exp6.py --shards 4 --nodes 4
```

2x2 参考规模：

```bash
python scripts/plots/exp6/plot_exp6.py --shards 2 --nodes 2
```

自定义路径：

```bash
python scripts/plots/exp6/plot_exp6.py \
  --metrics-dir results/exp6_sensitivity/metrics \
  --output-dir results/figures/exp6 \
  --shards 4 --nodes 4
```

