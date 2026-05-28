package params

import (
	"fmt"
	"log"
	"strings"
)

// MigrationStrategy 账户迁移方法（维度一对比实验：一次运行仅一种）。
type MigrationStrategy string

const (
	// StrategyOriginal 原 BlockEmulator 工程近似路径（Not_Lock + CaP，非论文 MVSS）。
	StrategyOriginal MigrationStrategy = "original"
	// StrategyMVSS 论文 MVSS 实现（分流 + TX_sync + nonce/RedirectTag 等）。
	StrategyMVSS MigrationStrategy = "MVSS"
	// StrategyMVSSDelta 论文 MVSS-Delta（在 MVSS 基础上启用增量同步，Phase2）。
	StrategyMVSSDelta MigrationStrategy = "MVSS-Delta"
	// StrategySOTALock 全锁基线（Fine-tuned Lock 论文中的 SOTA Lock；非 LB-Chain）。
	StrategySOTALock MigrationStrategy = "SOTA-Lock"
	// StrategyFineTunedLock 半锁基线（Huang et al. INFOCOM'24 Fine-tuned Lock）。
	StrategyFineTunedLock MigrationStrategy = "Fine-tuned-Lock"
	StrategyStopEpoch MigrationStrategy = "stop_epoch"

	// 过渡期别名（与上列 canonical 字符串相同，Parse 时统一归一）。
	StrategyLock      = StrategySOTALock
	StrategyFinetuned = StrategyFineTunedLock
)

// ParseMigrationStrategy 解析命令行或配置中的策略名；非法值 panic。
func ParseMigrationStrategy(s string) MigrationStrategy {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "original":
		return StrategyOriginal
	case "mvss":
		return StrategyMVSS
	case "mvss+", "mvss_plus", "mvssplus":
		return StrategyMVSS
	case "mvss-delta", "mvss_delta", "mvssdelta":
		return StrategyMVSSDelta
	case "sota-lock", "sota_lock", "sotalock", "lock":
		return StrategySOTALock
	case "fine-tuned-lock", "fine_tuned_lock", "finetunedlock", "finetuned":
		return StrategyFineTunedLock
	case "stop_epoch":
		return StrategyStopEpoch
	default:
		switch MigrationStrategy(strings.TrimSpace(s)) {
		case StrategyOriginal, StrategyMVSS, StrategyMVSSDelta,
			StrategySOTALock, StrategyFineTunedLock, StrategyStopEpoch:
			return MigrationStrategy(strings.TrimSpace(s))
		}
		log.Panic(fmt.Sprintf(
			"未知 MigrationStrategy: %q，可选: original, MVSS, MVSS-Delta, SOTA-Lock, Fine-tuned-Lock, stop_epoch（别名 lock/finetuned/MVSS+）",
			s))
	}
	return ""
}

// ApplyMigrationStrategy 根据 MigrationStrategy 同步 Stop/Lock/Not_Lock 三个 bool（过渡期供现有代码使用）。
func ApplyMigrationStrategy(cfg *ChainConfig) {
	if cfg == nil {
		return
	}
	if cfg.MigrationStrategy == "" {
		cfg.MigrationStrategy = StrategyOriginal
	}
	switch cfg.MigrationStrategy {
	case StrategyMVSS, StrategyMVSSDelta:
		cfg.Stop_When_Migrating = false
		cfg.Lock_Acc_When_Migrating = false
		cfg.Not_Lock_Acc_When_Migrating = true
	case StrategyOriginal:
		cfg.Stop_When_Migrating = false
		cfg.Lock_Acc_When_Migrating = false
		cfg.Not_Lock_Acc_When_Migrating = true
	case StrategySOTALock:
		cfg.Stop_When_Migrating = false
		cfg.Lock_Acc_When_Migrating = true
		cfg.Not_Lock_Acc_When_Migrating = false
	case StrategyFineTunedLock:
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

// IsMVSS 是否为论文 MVSS 协议分支（含 MVSS-Delta，Delta 在其上扩展 sync）。
func IsMVSS() bool {
	if Config == nil {
		return false
	}
	switch Config.MigrationStrategy {
	case StrategyMVSS, StrategyMVSSDelta:
		return true
	default:
		return false
	}
}

// IsMVSSDelta 是否为 MVSS-Delta 策略（Phase2 增量同步入口）。
func IsMVSSDelta() bool {
	return Config != nil && Config.MigrationStrategy == StrategyMVSSDelta
}

// IsSOTALock 是否为 SOTA Lock 全锁基线。
func IsSOTALock() bool {
	return Config != nil && Config.MigrationStrategy == StrategySOTALock
}

// IsFineTunedLock 是否为 Fine-tuned Lock 半锁基线。
func IsFineTunedLock() bool {
	return Config != nil && Config.MigrationStrategy == StrategyFineTunedLock
}

// IsMVSSPlus 过渡期别名，等同 IsMVSS。
func IsMVSSPlus() bool {
	return IsMVSS()
}

func init() {
	if Config != nil {
		ApplyMigrationStrategy(Config)
	}
}
