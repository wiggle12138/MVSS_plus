# Sync 探针实验分析报告

**总体结论: 通过**
- 备注: 9 笔探针交易 isSuccess=false（合成转账常见，以上链块高为准）

## 1. 探针交易上链分析

- 探针交易总数: **9**
- 探针账户数: **3**
- 全局块序验收: **通过**
- 全局块序: tx1_old@S0#B22 → tx1_old@S0#B22 → tx1_old@S0#B22 → tx2_new@S1#B24 → tx2_new@S1#B24 → tx2_new@S1#B24 → tx3_old@S0#B25 → tx3_old@S0#B25 → tx3_old@S0#B25

### 1.1 按 ClientTS 逻辑序（old → new → old）

| account | probe_type | client_ts | txid | shard | block | request_time | ok |
|---:|---|---:|---:|---|---:|---:|---|
| 0 | tx1_old | 100 | 9000000001 | S0 | 22 | -1780902967999 | False |
| 0 | tx2_new | 200 | 9000000002 | S1 | 24 | 49070 | False |
| 0 | tx3_old | 300 | 9000000003 | S0 | 25 | -1780902967999 | False |
| 1 | tx1_old | 100 | 9000000011 | S0 | 22 | -1780902967999 | False |
| 1 | tx2_new | 200 | 9000000012 | S1 | 24 | 49070 | False |
| 1 | tx3_old | 300 | 9000000013 | S0 | 25 | -1780902967999 | False |
| 2 | tx1_old | 100 | 9000000021 | S0 | 22 | -1780902967999 | False |
| 2 | tx2_new | 200 | 9000000022 | S1 | 24 | 49070 | False |
| 2 | tx3_old | 300 | 9000000023 | S0 | 25 | -1780902967999 | False |

### 1.2 按全局上链块高（实际执行序）

| order | probe_type | shard | block | txid | client_ts |
|---:|---|---|---:|---:|---:|
| 1 | tx1_old | S0 | 22 | 9000000001 | 100 |
| 2 | tx1_old | S0 | 22 | 9000000011 | 100 |
| 3 | tx1_old | S0 | 22 | 9000000021 | 100 |
| 4 | tx2_new | S1 | 24 | 9000000002 | 200 |
| 5 | tx2_new | S1 | 24 | 9000000012 | 200 |
| 6 | tx2_new | S1 | 24 | 9000000022 | 200 |
| 7 | tx3_old | S0 | 25 | 9000000003 | 300 |
| 8 | tx3_old | S0 | 25 | 9000000013 | 300 |
| 9 | tx3_old | S0 | 25 | 9000000023 | 300 |

### 1.3 分账户验收

**账户 0** (`f341cafc9ba3d91e…`) — 通过
- 块序: tx1@B22 < tx2@B24 < tx3@B25

**账户 1** (`96904d8c1f1c88df…`) — 通过
- 块序: tx1@B22 < tx2@B24 < tx3@B25

**账户 2** (`9e9ff119553d7ad5…`) — 通过
- 块序: tx1@B22 < tx2@B24 < tx3@B25

## 2. Sync 消息通信时序（按 ts 全局排序）

- 事件总数: **14**
- 通路验收: **通过**

```
01 +    0ms  S0  send      delta [S0 → S1]  [OK] batch=3
02 +    0ms  S1  recv      delta [S0 → S1]  [OK] batch=3
03 + 2063ms  S1  apply     delta [S1 本地] f341cafc9ba3… n0→1  [OK] nonce=1
04 + 2065ms  S1  apply     delta [S1 本地] 96904d8c1f1c… n0→1  [OK] nonce=1
05 + 2066ms  S1  apply     delta [S1 本地] 9e9ff119553d… n1→2  [OK] nonce=2
06 + 4030ms  S0  recv      delta  [OK]
07 + 4030ms  S0  recv      delta  [OK]
08 + 4030ms  S0  recv      delta  [OK]
09 + 4030ms  S1  ack_send  delta [S1 → S0] 96904d8c1f1c… n1→2  [OK]
10 + 4030ms  S1  ack_send  delta [S1 → S0] 9e9ff119553d… n2→3  [OK]
11 + 4030ms  S1  ack_send  delta [S1 → S0] f341cafc9ba3… n1→2  [OK]
12 + 4031ms  S0  ack_recv  delta [S1 → S0] f341cafc9ba3… n1→2  [OK] nonce=2
13 + 4032ms  S0  ack_recv  delta [S1 → S0] 96904d8c1f1c… n1→2  [OK] nonce=2
14 + 4032ms  S0  ack_recv  delta [S1 → S0] 9e9ff119553d… n2→3  [OK] nonce=3
```

期望主链路：`S0 send → S1 recv → S1 apply → S1 ack_send → S0 ack_recv`

## 3. 输出文件

- `probe_tx_detail.csv` — 探针交易明细（含 probe_type / client_ts）
- `sync_timeline.csv` — 合并排序后的 sync 事件
