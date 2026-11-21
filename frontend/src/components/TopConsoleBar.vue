<script setup lang="ts">
import type { PresetUser, SystemStatus } from '../types'

const props = defineProps<{
  status: SystemStatus | null
  tps: number
  selectedUserId: number
  users: PresetUser[]
  busyAction: boolean
}>()

const emit = defineEmits<{
  'update:selectedUserId': [userId: number]
  toggleSimulator: []
  setRate: [intervalMs: number]
  emitOnce: []
  crashStorm: []
}>()

const ratePresets = [
  { label: '500ms 慢', value: 500 },
  { label: '200ms 标准', value: 200 },
  { label: '50ms 高速', value: 50 },
  { label: '15ms 极速', value: 15 },
]

function onUserChange(event: Event) {
  const target = event.target as HTMLSelectElement
  emit('update:selectedUserId', Number(target.value))
}
</script>

<template>
  <header class="panel top-bar">
    <div class="top-row">
      <div class="brand">
        <div class="brand-title">
          <span class="brand-mark">PP</span>
          <span>PricePulse V3</span>
          <span class="badge badge-sky">实时监控控制台</span>
        </div>
        <div class="brand-sub">
          {{ props.status?.lanes ?? 16 }} 路分片队列 · ZSet 预筛 · Outbox 租约 · RocketMQ 削峰
        </div>
      </div>

      <div class="metrics-strip">
        <div class="metric-card">
          <div class="metric-label">当前匹配 TPS</div>
          <div class="metric-value mono">{{ props.tps.toFixed(1) }} <span class="metric-unit">ticks/s</span></div>
          <div class="metric-sub mono">累计处理 {{ props.status?.processedQuotes ?? 0 }}</div>
        </div>

        <div class="metric-card">
          <div class="metric-label">Redis 匹配状态</div>
          <div class="metric-status-row">
            <span
              v-if="!props.status || props.status.matchingMode === 'REDIS'"
              class="badge badge-emerald"
            >
              <span class="dot dot-emerald"></span>
              正常 · Redis ZSet
            </span>
            <span v-else class="badge badge-amber">
              <span class="dot dot-amber"></span>
              熔断降级中 · MySQL
            </span>
          </div>
          <div class="metric-sub mono">
            CB: {{ props.status?.circuitBreakerState ?? '-' }} · R {{ props.status?.redisMatchCount ?? 0 }} / DB
            {{ props.status?.fallbackMatchCount ?? 0 }}
          </div>
        </div>

        <div class="metric-card">
          <div class="metric-label">Outbox 待发送积压</div>
          <div class="metric-value mono" :class="{ 'text-amber': (props.status?.outboxPendingCount ?? 0) > 0 }">
            {{ props.status?.outboxPendingCount ?? 0 }}
          </div>
          <div class="metric-sub mono">
            已投递 {{ props.status?.outboxPublishedCount ?? 0 }} · 消费 {{ props.status?.consumedNotificationCount ?? 0 }}
          </div>
        </div>

        <div class="metric-card">
          <div class="metric-label">MySQL 读查询计数</div>
          <div class="metric-value mono">{{ props.status?.dbQueryCount ?? 0 }}</div>
          <div class="metric-sub mono">
            K线落库 {{ props.status?.flushedKlineBars ?? 0 }} · 触发 {{ props.status?.triggeredRuleCount ?? 0 }}
          </div>
        </div>
      </div>
    </div>

    <div class="controls-row">
      <div class="control-group">
        <label class="control-label" for="user-select">当前用户</label>
        <select
          id="user-select"
          class="select"
          :value="props.selectedUserId"
          @change="onUserChange"
        >
          <option v-for="u in props.users" :key="u.id" :value="u.id">
            {{ u.label }}
          </option>
        </select>
      </div>

      <div class="control-group">
        <button
          class="btn"
          :class="props.status?.simulatorRunning ? 'btn-emerald' : 'btn-primary'"
          :disabled="props.busyAction"
          @click="emit('toggleSimulator')"
        >
          {{ props.status?.simulatorRunning ? '⏸ 暂停行情流' : '▶ 启动行情流' }}
        </button>

        <div class="rate-pills">
          <button
            v-for="preset in ratePresets"
            :key="preset.value"
            class="btn btn-sm"
            :class="{ 'btn-active-pill': props.status?.simulatorIntervalMs === preset.value }"
            :disabled="props.busyAction"
            @click="emit('setRate', preset.value)"
          >
            {{ preset.label }}
          </button>
        </div>

        <button
          class="btn btn-sm"
          :disabled="props.busyAction"
          @click="emit('emitOnce')"
        >
          单次发 100 股
        </button>
      </div>

      <button
        class="btn btn-danger"
        :disabled="props.busyAction"
        @click="emit('crashStorm')"
      >
        ⚡ 全市场暴跌 5% 告警风暴
      </button>
    </div>
  </header>
</template>

<style scoped>
.top-bar {
  padding: 12px 16px;
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.top-row {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.brand-title {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 16px;
  font-weight: 700;
  letter-spacing: -0.01em;
}

.brand-mark {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 26px;
  height: 26px;
  border-radius: 7px;
  background: linear-gradient(135deg, #38bdf8, #2563eb);
  color: #fff;
  font-size: 12px;
  font-weight: 800;
}

.brand-sub {
  margin-top: 3px;
  color: var(--text-muted);
  font-size: 12px;
}

.metrics-strip {
  display: grid;
  grid-template-columns: repeat(4, minmax(175px, 1fr));
  gap: 10px;
  flex: 1;
  max-width: 920px;
}

@media (max-width: 1120px) {
  .metrics-strip {
    grid-template-columns: repeat(2, minmax(160px, 1fr));
    max-width: none;
    width: 100%;
  }
}

.metric-card {
  background: var(--bg-elevated);
  border: 1px solid var(--border-subtle);
  border-radius: 8px;
  padding: 8px 11px;
}

.metric-label {
  font-size: 11px;
  color: var(--text-secondary);
  margin-bottom: 3px;
}

.metric-value {
  font-size: 18px;
  font-weight: 700;
  color: var(--text-primary);
}

.metric-unit {
  font-size: 11px;
  font-weight: 500;
  color: var(--text-muted);
}

.metric-status-row {
  margin: 2px 0 3px;
}

.metric-sub {
  font-size: 11px;
  color: var(--text-muted);
  margin-top: 2px;
}

.text-amber {
  color: var(--accent-amber);
}

.controls-row {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  padding-top: 8px;
  border-top: 1px solid var(--border-subtle);
}

.control-group {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 8px;
}

.control-label {
  color: var(--text-secondary);
  font-size: 12px;
  font-weight: 500;
}

.rate-pills {
  display: inline-flex;
  gap: 4px;
}
</style>
