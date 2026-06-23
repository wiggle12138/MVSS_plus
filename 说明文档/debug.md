# 开发变更记录（精简）

早期联调（5.12–5.24：空块、创世树列错位、首块门控、迁移期 bug 等）已收敛进主分支，此处仅保留 **策略与 Stage3** 相关里程碑。细节见 [账户迁移策略对比.md](./账户迁移策略对比.md)、[Sync探针注入.md](./Sync探针注入.md)、[聚合窗口.md](./聚合窗口.md)。

---

## 5.25 — MVSS 主线（Phase1）

| # | 内容 |
|---|------|
| 7–13 | Nonce / RedirectTag；TXmig1 Sync+OrderList；`TXsync` 与 `mvss_ctx` FSM；分流替代锁池；`IsMVSS()` 策略分叉 |
| 14–15 | 死锁修复；缺状态账户降级 |
| 16 | 策略重命名：`original` / `MVSS` / `MVSS-Delta`；`-m MVSS+` 仍解析为 `MVSS` |

## 5.26 — 基线别名

| # | 内容 |
|---|------|
| 17 | `SOTA-Lock` / `Fine-tuned-Lock` canonical 名；参数说明合并至 [参数配置.md](./参数配置.md) |

## 5.27 — MVSS-Delta MVP

| # | 内容 |
|---|------|
| 18–21 | `TXsyncDelta` 收发 apply/ack；失败 abort 无 fallback；`S*_sync.csv` 日志 |

## 5.28 — client_ts 双时间戳

| # | 内容 |
|---|------|
| 22–26 | `ClientTimestamp` + `OrderTimestamp()`；`DetectInterleave` 用 client_ts 排序、RequestTime 判 old/new |
| 27–31 | `datasets/mvss_interleave_benchmark.csv`；换数据集须清 `*_blockchain_db*` |

## 6.4 — Sync 探针 Stage3 联调

| # | 内容 |
|---|------|
| 32 | PhaseB 晚到 new：`mvssPromoteMigNewTxsToHead` |
| 33 | `sync.csv` 并发写修复；bat 注入 `SYNC_PROBE=1` |
| 34 | State_ini 出站 batch（`DeltaAggregateWindowMs`）；见 [聚合窗口.md](./聚合窗口.md) |
