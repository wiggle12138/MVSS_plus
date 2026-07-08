# Exp5：资源开销细粒度实验

## Question：我要回答什么问题？

- `MVSS-Delta` 是否在同步阶段降低资源消耗（网络/计算/内存）？
- 这种优化是否伴随额外系统代价？

## Hypothesis：我预计结果是什么，为什么？

- 预计 Delta 会显著降低同步传输字节和消息压力；
- 预计在正确性不退化前提下，资源代理指标优于全量同步路径。

## Design：变量/控制量/指标/验收口径是什么？

### P1（当前可执行，资源代理）

- 对比：`MVSS` vs `MVSS-Delta`
- 开关：`EnableSyncProbe=true`
- 指标：`sync_bandwidth_mb`、`sync_send_count`、`dsr`、`avg_queue_len`

### P2（待开发，真实 profiling）

- 新增采样：pprof CPU/Heap、验证耗时埋点
- 指标：同步阶段 CPU 占比、峰值内存、验证耗时分布

### 验收口径

- P1：迁移触发 + sync 闭环 + 指标文件完整
- P2：存在 profiling 文件并可形成定量结论

## Result：数据是否支持假设？统计上/趋势上如何？

- 当前状态：P1 可执行，P2 待开发。
- 结果记录：
  - P1：按 run 均值对比资源代理指标；
  - P2：待埋点后填写 CPU/内存结论。

## Interpretation & Threats：机理解释 + 有哪些限制/混杂因素？

- 机理解释候选：
  - Delta 同步减少传输内容，降低网络与处理负担；
  - 同步链路轻量化可减少迁移期间的系统抖动。
- 限制：
  - P1 是代理指标，不等同完整资源剖析；
  - P2 未落地前，不能做论文级资源开销强结论。

## 产物目录约定

```text
results/
  exp5_resource/
    raw/phase_{P1_or_P2}/strategy_{STR}/run{K}/
    metrics/exp5_{phase}_{STR}_run{K}.json
    summary/exp5_resource_summary.md
```
