<script setup lang="ts">
import type { Stock } from '../types'

export interface CacheProbeResult {
  symbol: string
  statusCode: number
  found: boolean
  dbQueriesDelta: number
  dbQueriesTotal: number
  message: string
}

const props = defineProps<{
  stocks: Stock[]
  selectedSymbol: string
  searchQuery: string
  probeResult: CacheProbeResult | null
  probing: boolean
}>()

const emit = defineEmits<{
  'update:searchQuery': [value: string]
  selectStock: [symbol: string]
  probeSymbol: [symbol: string]
}>()

function onInput(event: Event) {
  const target = event.target as HTMLInputElement
  emit('update:searchQuery', target.value)
}

function handleProbeClick() {
  const target = props.searchQuery.trim() || props.selectedSymbol
  if (target) {
    emit('probeSymbol', target.toUpperCase())
  }
}

function handleEnter() {
  const trimmed = props.searchQuery.trim()
  if (!trimmed) return
  if (props.stocks.length === 1) {
    emit('selectStock', props.stocks[0].symbol)
    return
  }
  emit('probeSymbol', trimmed.toUpperCase())
}
</script>

<template>
  <aside class="panel sidebar">
    <div class="sidebar-header">
      <div class="sidebar-title">
        <span>美股标的池</span>
        <span class="badge badge-muted mono">{{ props.stocks.length }}</span>
      </div>

      <div class="search-row">
        <input
          class="input search-input"
          type="text"
          :value="props.searchQuery"
          placeholder="搜索代码或测试非法代码 (如 FAKE99)"
          @input="onInput"
          @keyup.enter="handleEnter"
        />
        <button
          class="btn btn-sm btn-primary"
          :disabled="props.probing"
          title="直查 /api/quotes/{symbol} 验证缓存穿透防御"
          @click="handleProbeClick"
        >
          {{ props.probing ? '探测...' : '直查' }}
        </button>
      </div>

      <div v-if="props.probeResult" class="probe-card">
        <div class="probe-head">
          <span class="mono">{{ props.probeResult.symbol }}</span>
          <span
            class="badge mono"
            :class="props.probeResult.found ? 'badge-emerald' : 'badge-rose'"
          >
            HTTP {{ props.probeResult.statusCode }}
          </span>
        </div>
        <div class="probe-msg">{{ props.probeResult.message }}</div>
        <div class="probe-delta mono">
          MySQL 查询增量: <strong>+{{ props.probeResult.dbQueriesDelta }}</strong> (总计
          {{ props.probeResult.dbQueriesTotal }})
        </div>
      </div>
    </div>

    <div class="stock-list">
      <button
        v-for="stock in props.stocks"
        :key="stock.id"
        class="stock-item"
        :class="{ active: stock.symbol === props.selectedSymbol }"
        @click="emit('selectStock', stock.symbol)"
      >
        <span class="stock-symbol mono">{{ stock.symbol }}</span>
        <span class="stock-name">{{ stock.name }}</span>
      </button>

      <div v-if="props.stocks.length === 0" class="empty-stocks">
        <p>本地标的池无匹配股票。</p>
        <p class="empty-hint">
          可点击上方「直查」请求后端验证不存在代码的 <code>__NULL__</code> 防穿透空值缓存。
        </p>
      </div>
    </div>
  </aside>
</template>

<style scoped>
.sidebar {
  display: flex;
  flex-direction: column;
  overflow: hidden;
  max-height: calc(100vh - 148px);
}

.sidebar-header {
  padding: 12px;
  border-bottom: 1px solid var(--border-subtle);
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.sidebar-title {
  display: flex;
  align-items: center;
  justify-content: space-between;
  font-weight: 600;
  font-size: 13px;
}

.search-row {
  display: flex;
  gap: 6px;
}

.search-input {
  flex: 1;
  min-width: 0;
  font-size: 12px;
}

.probe-card {
  background: var(--bg-elevated);
  border: 1px solid var(--border-strong);
  border-radius: 7px;
  padding: 8px;
  font-size: 11px;
  display: flex;
  flex-direction: column;
  gap: 3px;
}

.probe-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  font-weight: 600;
}

.probe-msg {
  color: var(--text-secondary);
}

.probe-delta {
  color: var(--accent-sky);
}

.stock-list {
  flex: 1;
  overflow-y: auto;
  padding: 6px;
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.stock-item {
  width: 100%;
  text-align: left;
  background: transparent;
  border: 1px solid transparent;
  border-radius: 6px;
  padding: 7px 9px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  color: var(--text-primary);
  cursor: pointer;
  transition: background 0.12s ease, border-color 0.12s ease;
}

.stock-item:hover {
  background: var(--bg-elevated);
}

.stock-item.active {
  background: rgba(56, 189, 248, 0.14);
  border-color: rgba(56, 189, 248, 0.45);
}

.stock-symbol {
  font-weight: 700;
  color: var(--text-primary);
}

.stock-name {
  color: var(--text-muted);
  font-size: 11px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  max-width: 165px;
}

.empty-stocks {
  padding: 20px 10px;
  text-align: center;
  color: var(--text-secondary);
}

.empty-hint {
  margin-top: 6px;
  font-size: 11px;
  color: var(--text-muted);
}
</style>
