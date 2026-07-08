# Exp3：安全攻击防御实验（双花 / 重放）

## Question：我要回答什么问题？

- 迁移期间，系统能否有效防御重放与双花攻击？
- 防御机制会带来多大性能代价？

## Hypothesis：我预计结果是什么，为什么？

- 预计依赖 `nonce + RedirectTag + MigCtx` 的校验可以阻断大部分重放/双花。
- 预计防御代价存在但可接受，主要体现在局部延迟与验证开销。

## Design：变量/控制量/指标/验收口径是什么？

### 当前可执行层（L1：健康性）

- 对比策略：`MVSS`、`MVSS-Delta`
- 目标：验证防御链路在迁移场景下稳定、不出现明显异常。
- 指标：`probe_ok`、`abort,delta`、日志中的 nonce 相关错误率。

### 待开发层（L2：攻击注入）

- 攻击场景：`replay_redirected_tx`、`double_spend_race`
- 需要新增：攻击注入模式、攻击结果标签、拒绝原因统计。
- 指标：`attack_success_rate`、`attack_reject_rate`、`defense_overhead_latency_ms`。

## Result：数据是否支持假设？统计上/趋势上如何？

- 当前状态：仅可做 L1（健康性）结论；L2 尚待代码支持。
- 结论模板：
  - L1：通过/不通过；
  - L2：待实现后填写攻击成功率与防御代价。

## Interpretation & Threats：机理解释 + 有哪些限制/混杂因素？

- 机理解释候选：
  - 重定向校验与 nonce 线性推进约束可抑制重放；
  - 迁移上下文状态机降低跨片状态歧义。
- 限制：
  - 无攻击注入器时不能得出论文级“攻击防御有效率”结论；
  - 仅健康性日志不等于安全性充分证明。

## 产物目录约定

```text
results/
  exp3_security/
    raw/layer_{L1_or_L2}/attack_{A}/strategy_{STR}/run{K}/
    metrics/exp3_{layer}_{attack}_{STR}_run{K}.json
    summary/exp3_security_summary.md
```
