<script setup lang="ts">
import { ref } from 'vue'
import type { AlertRule, Direction, QuoteSnapshot } from '../types'

const props = defineProps<{
  symbol: string
  userId: number
  quote: QuoteSnapshot | null
  rules: AlertRule[]
  submitting: boolean
  errorMessage: string | null
}>()

const emit = defineEmits<{
  createRule: [payload: { direction: Direction; thresholdPrice: number }]
  updateRule: [payload: { id: number; direction: Direction; thresholdPrice: number; version: number }]
  deleteRule: [payload: { id: number; version: number }]
  clearError: []
}>()

const newDirection = ref<Direction>('BELOW')
const newPrice = ref<string>('')

const editingRuleId = ref<number | null>(null)
const editDirection = ref<Direction>('ABOVE')
const editPrice = ref<string>('')
const editVersion = ref<number>(0)

function applyPreset(direction: Direction, multiplier: number) {
  emit('clearError')
  newDirection.value = direction
  if (props.quote) {
    newPrice.value = (props.quote.price * multiplier).toFixed(2)
  }
}

function handleCreate() {
  emit('clearError')
  const numeric = Number(newPrice.value)
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return
  }
  emit('createRule', {
    direction: newDirection.value,
    thresholdPrice: Number(numeric.toFixed(2)),
  })
}

function startEdit(rule: AlertRule) {
  emit('clearError')
  editingRuleId.value = rule.id
  editDirection.value = rule.direction
  editPrice.value = rule.thresholdPrice.toFixed(2)
  editVersion.value = rule.version
}

function cancelEdit() {
  editingRuleId.value = null
}

function saveEdit(ruleId: number) {
  emit('clearError')
  const numeric = Number(editPrice.value)
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return
  }
  emit('updateRule', {
    id: ruleId,
    direction: editDirection.value,
    thresholdPrice: Number(numeric.toFixed(2)),
    version: editVersion.value,
  })
  editingRuleId.value = null
}
</script>

<template>
  <section class="panel rule-panel">
    <div class="rule-header">
      <div>
        <h2 class="section-title">{{ props.symbol }} 目标价规则（User {{ props.userId }}）</h2>
        <div class="section-sub">一次性触发（ACTIVE → COMPLETED），支持乐观锁版本控制与现价校验</div>
      </div>
      <div class="quick-presets" v-if="props.quote">
        <button
          type="button"
          class="btn btn-sm btn-emerald"
          @click="applyPreset('ABOVE', 1.02)"
        >
          +2% 涨破
        </button>
        <button
          type="button"
          class="btn btn-sm"
          @click="applyPreset('BELOW', 0.98)"
        >
          -2% 跌破
        </button>
      </div>
    </div>

    <form class="create-form" @submit.prevent="handleCreate">
      <select v-model="newDirection" class="select">
        <option value="ABOVE">ABOVE 涨破 ≥</option>
        <option value="BELOW">BELOW 跌破 ≤</option>
      </select>

      <input
        v-model="newPrice"
        class="input price-input mono"
        type="number"
        step="0.01"
        min="0.01"
        placeholder="输入目标价 (如 150.00)"
        required
      />

      <button type="submit" class="btn btn-primary" :disabled="props.submitting">
        {{ props.submitting ? '提交中...' : '创建预警规则' }}
      </button>
    </form>

    <div v-if="props.errorMessage" class="error-banner">
      <span>{{ props.errorMessage }}</span>
      <button type="button" class="close-btn" @click="emit('clearError')">✕</button>
    </div>

    <div class="rules-list">
      <div v-for="rule in props.rules" :key="rule.id" class="rule-row">
        <template v-if="editingRuleId === rule.id">
          <div class="edit-row">
            <select v-model="editDirection" class="select">
              <option value="ABOVE">ABOVE</option>
              <option value="BELOW">BELOW</option>
            </select>
            <input
              v-model="editPrice"
              class="input price-input mono"
              type="number"
              step="0.01"
              min="0.01"
            />
            <button type="button" class="btn btn-sm btn-emerald" @click="saveEdit(rule.id)">
              保存
            </button>
            <button type="button" class="btn btn-sm" @click="cancelEdit">取消</button>
          </div>
        </template>

        <template v-else>
          <div class="rule-main">
            <span
              class="badge"
              :class="rule.direction === 'ABOVE' ? 'badge-emerald' : 'badge-rose'"
            >
              {{ rule.direction === 'ABOVE' ? '涨破 ≥' : '跌破 ≤' }}
            </span>
            <span class="rule-price mono">${{ rule.thresholdPrice.toFixed(2) }}</span>
            <span
              class="badge"
              :class="rule.status === 'ACTIVE' ? 'badge-sky' : 'badge-muted'"
            >
              {{ rule.status }}
            </span>
            <span class="rule-meta mono">v{{ rule.version }} · #{{ rule.id }}</span>
          </div>

          <div class="rule-actions">
            <button
              v-if="rule.status === 'ACTIVE'"
              type="button"
              class="btn btn-sm"
              :disabled="props.submitting"
              @click="startEdit(rule)"
            >
              编辑
            </button>
            <button
              v-if="rule.status === 'ACTIVE'"
              type="button"
              class="btn btn-sm"
              :disabled="props.submitting"
              @click="emit('deleteRule', { id: rule.id, version: rule.version })"
            >
              删除
            </button>
          </div>
        </template>
      </div>

      <div v-if="props.rules.length === 0" class="empty-rules">
        当前股票暂无规则。可使用上方「+2% 涨破 / -2% 跌破」快捷填入或手动创建。
      </div>
    </div>
  </section>
</template>

<style scoped>
.rule-panel {
  padding: 14px 16px;
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.rule-header {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.section-title {
  margin: 0;
  font-size: 14px;
  font-weight: 700;
}

.section-sub {
  margin-top: 2px;
  font-size: 11px;
  color: var(--text-muted);
}

.quick-presets {
  display: flex;
  gap: 6px;
}

.create-form {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  align-items: center;
}

.price-input {
  width: 170px;
}

.error-banner {
  background: rgba(244, 63, 94, 0.14);
  border: 1px solid rgba(244, 63, 94, 0.45);
  color: #fecdd3;
  padding: 8px 10px;
  border-radius: 6px;
  font-size: 12px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.close-btn {
  background: transparent;
  border: none;
  color: inherit;
  cursor: pointer;
}

.rules-list {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.rule-row {
  background: var(--bg-elevated);
  border: 1px solid var(--border-subtle);
  border-radius: 7px;
  padding: 8px 10px;
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.rule-main {
  display: flex;
  align-items: center;
  gap: 8px;
}

.rule-price {
  font-size: 14px;
  font-weight: 700;
}

.rule-meta {
  font-size: 11px;
  color: var(--text-muted);
}

.rule-actions,
.edit-row {
  display: flex;
  align-items: center;
  gap: 6px;
}

.empty-rules {
  text-align: center;
  padding: 18px 8px;
  color: var(--text-muted);
  font-size: 12px;
}
</style>
