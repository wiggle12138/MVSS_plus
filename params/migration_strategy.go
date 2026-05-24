package params

import (
	"fmt"
	"log"
	"strings"
)

// MigrationStrategy 账户迁移方法（维度一对比实验：一次运行仅一种）。
type MigrationStrategy string

const (
	// StrategyMVSSPlus 新方法 MVSS+（分流 + TX_sync 等，逐步实现；接口入口）。
	StrategyMVSSPlus MigrationStrategy = "MVSS+"
	// StrategyMVSS 论文 MVSS 对齐目标；当前尚未完整实现，暂走现有工程逻辑（Not_Lock + CaP）。
	StrategyMVSS MigrationStrategy = "MVSS"
	StrategyLock         MigrationStrategy = "lock"
	StrategyFinetuned    MigrationStrategy = "finetuned"
	StrategyStopEpoch    MigrationStrategy = "stop_epoch"
)

// ParseMigrationStrategy 解析命令行或配置中的策略名；非法值 panic。
// 接受 MVSS、MVSS+，以及别名 mvss、mvss_plus（便于 shell 转义）。
func ParseMigrationStrategy(s string) MigrationStrategy {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "mvss":
		return StrategyMVSS
	case "mvss+", "mvss_plus", "mvssplus":
		return StrategyMVSSPlus
	case "lock":
		return StrategyLock
	case "finetuned":
		return StrategyFinetuned
	case "stop_epoch":
		return StrategyStopEpoch
	default:
		// 保留大小写敏感字面量 MVSS / MVSS+
		switch MigrationStrategy(strings.TrimSpace(s)) {
		case StrategyMVSS, StrategyMVSSPlus, StrategyLock, StrategyFinetuned, StrategyStopEpoch:
			return MigrationStrategy(strings.TrimSpace(s))
		}
		log.Panic(fmt.Sprintf("未知 MigrationStrategy: %q，可选: MVSS, MVSS+, lock, finetuned, stop_epoch", s))
	}
	return ""
}

// ApplyMigrationStrategy 根据 MigrationStrategy 同步 Stop/Lock/Not_Lock 三个 bool（过渡期供现有代码使用）。
func ApplyMigrationStrategy(cfg *ChainConfig) {
	if cfg == nil {
		return
	}
	if cfg.MigrationStrategy == "" {
		cfg.MigrationStrategy = StrategyMVSS
	}
	switch cfg.MigrationStrategy {
	case StrategyMVSSPlus, StrategyMVSS:
		// MVSS+：后续在此策略下启用新逻辑；当前与 MVSS 相同 bool，便于先跑通联调。
		// MVSS：论文对齐接口，暂用现有 Not_Lock 工程路径。
		cfg.Stop_When_Migrating = false
		cfg.Lock_Acc_When_Migrating = false
		cfg.Not_Lock_Acc_When_Migrating = true
	case StrategyLock:
		cfg.Stop_When_Migrating = false
		cfg.Lock_Acc_When_Migrating = true
		cfg.Not_Lock_Acc_When_Migrating = false
	case StrategyFinetuned:
		cfg.Stop_When_Migrating = false
		cfg.Lock_Acc_When_Migrating = false
		cfg.Not_Lock_Acc_When_Migrating = false
	case StrategyStopEpoch:
		cfg.Stop_When_Migrating = true
		cfg.Lock_Acc_When_Migrating = false
		cfg.Not_Lock_Acc_When_Migrating = false
	default:
		log.Panic(fmt.Sprintf("未知 MigrationStrategy: %q", cfg.MigrationStrategy))
	}
}

func init() {
	if Config != nil {
		ApplyMigrationStrategy(Config)
	}
}
