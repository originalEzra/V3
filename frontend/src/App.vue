<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import {
  ApiError,
  buildNotificationsWsUrl,
  createRule,
  deleteRule,
  emitSimulatorOnce,
  fetchKlines,
  fetchNotifications,
  fetchQuote,
  fetchRules,
  fetchStocks,
  fetchSystemStatus,
  fetchUnreadCount,
  markAllNotificationsRead,
  markNotificationRead,
  startSimulator,
  stopSimulator,
  triggerCrashStorm,
  updateRule,
  updateSimulatorRate,
} from './api'
import KlineChart from './components/KlineChart.vue'
import NotificationCenter from './components/NotificationCenter.vue'
import RulePanel from './components/RulePanel.vue'
import StockSidebar, { type CacheProbeResult } from './components/StockSidebar.vue'
import TopConsoleBar from './components/TopConsoleBar.vue'
import {
  PRESET_USERS,
  type AlertRule,
  type Direction,
  type KlineBar,
  type NotificationItem,
  type QuoteSnapshot,
  type Stock,
  type SystemStatus,
  type WsNotificationMessage,
} from './types'

const stocks = ref<Stock[]>([])
const selectedSymbol = ref<string>('AAPL')
const searchQuery = ref<string>('')
const selectedUserId = ref<number>(1)

const systemStatus = ref<SystemStatus | null>(null)
const liveTps = ref<number>(0)
let lastProcessedQuotes = 0
let lastSampleAt = 0

const currentQuote = ref<QuoteSnapshot | null>(null)
const currentKlines = ref<KlineBar[]>([])
const rules = ref<AlertRule[]>([])
const notifications = ref<NotificationItem[]>([])
const unreadCount = ref<number>(0)

const busyAction = ref<boolean>(false)
const submittingRule = ref<boolean>(false)
const ruleError = ref<string | null>(null)
const probing = ref<boolean>(false)
const probeResult = ref<CacheProbeResult | null>(null)
const toastMessage = ref<string | null>(null)
const wsConnected = ref<boolean>(false)

let statusPollTimer: number | null = null
let klinePollTimer: number | null = null
let searchTimer: number | null = null
let reconnectTimer: number | null = null
let toastTimer: number | null = null
let ws: WebSocket | null = null

const selectedStockName = computed(() => {
  const match = stocks.value.find((s) => s.symbol === selectedSymbol.value)
  return match?.name ?? selectedSymbol.value
})

const activeRules = computed(() => rules.value.filter((r) => r.status === 'ACTIVE'))

function showToast(message: string) {
  toastMessage.value = message
  if (toastTimer != null) {
    window.clearTimeout(toastTimer)
  }
  toastTimer = window.setTimeout(() => {
    toastMessage.value = null
  }, 4000)
}

async function loadStocks(query?: string) {
  try {
    stocks.value = await fetchStocks(query)
  } catch (err) {
    console.error('Failed loading stocks', err)
  }
}

async function refreshSystemStatus() {
  try {
    const next = await fetchSystemStatus()
    const now = Date.now()
    if (lastSampleAt > 0 && now > lastSampleAt) {
      const deltaQuotes = Math.max(0, next.processedQuotes - lastProcessedQuotes)
      const deltaSec = (now - lastSampleAt) / 1000
      liveTps.value = deltaQuotes / deltaSec
    }
    lastProcessedQuotes = next.processedQuotes
    lastSampleAt = now
    systemStatus.value = next
  } catch (err) {
    console.error('Failed loading system status', err)
  }
}

async function refreshQuoteAndRules() {
  const sym = selectedSymbol.value
  try {
    currentQuote.value = await fetchQuote(sym)
  } catch {
    currentQuote.value = null
  }
  try {
    rules.value = await fetchRules(selectedUserId.value, sym)
  } catch (err) {
    console.error('Failed loading rules', err)
  }
}

async function refreshKlines() {
  const sym = selectedSymbol.value
  try {
    currentKlines.value = await fetchKlines(sym, 60)
  } catch {
    currentKlines.value = []
  }
}

async function refreshNotifications() {
  const uid = selectedUserId.value
  try {
    const [list, unread] = await Promise.all([
      fetchNotifications(uid, 50),
      fetchUnreadCount(uid),
    ])
    notifications.value = list
    unreadCount.value = unread.unreadCount
  } catch (err) {
    console.error('Failed loading notifications', err)
  }
}

function connectWebSocket() {
  if (reconnectTimer != null) {
    window.clearTimeout(reconnectTimer)
    reconnectTimer = null
  }
  if (ws) {
    ws.onclose = null
    ws.close()
    ws = null
  }
  wsConnected.value = false
  const socket = new WebSocket(buildNotificationsWsUrl(selectedUserId.value))
  ws = socket

  socket.onopen = () => {
    wsConnected.value = true
  }

  socket.onmessage = (event) => {
    try {
      const payload = JSON.parse(event.data) as WsNotificationMessage
      if (payload.userId !== selectedUserId.value) return
      const exists = notifications.value.some((n) => n.id === payload.id)
      if (!exists) {
        notifications.value = [
          {
            id: payload.id,
            triggerId: payload.triggerId,
            userId: payload.userId,
            stockId: payload.stockId,
            content: payload.content,
            isRead: payload.isRead,
            createdAt: payload.createdAt ?? new Date().toISOString().slice(0, 19),
          },
          ...notifications.value,
        ].slice(0, 50)
        if (!payload.isRead) {
          unreadCount.value += 1
        }
      }
      showToast(`预警触发：${payload.content}`)
      void refreshQuoteAndRules()
      void refreshSystemStatus()
    } catch (err) {
      console.error('Invalid WebSocket payload', err)
    }
  }

  socket.onclose = () => {
    wsConnected.value = false
    if (ws === socket) {
      reconnectTimer = window.setTimeout(() => connectWebSocket(), 2000)
    }
  }

  socket.onerror = () => {
    wsConnected.value = false
  }
}

function handleSelectStock(symbol: string) {
  selectedSymbol.value = symbol
  ruleError.value = null
  void Promise.all([refreshQuoteAndRules(), refreshKlines()])
}

async function handleProbeSymbol(symbol: string) {
  probing.value = true
  try {
    const before = await fetchSystemStatus()
    let statusCode = 200
    let found = true
    let message = '命中有效股票并刷新最新价'
    try {
      const q = await fetchQuote(symbol)
      if (q) {
        message = `命中行情：$${q.price.toFixed(2)} (seq #${q.seq})`
      } else {
        message = '股票存在，当前尚无行情成交记录'
      }
    } catch (err) {
      if (err instanceof ApiError && err.status === 404) {
        statusCode = 404
        found = false
        message = '未知代码返回 404（重复直查将命中 Redis __NULL__ 空值缓存，DB 增量为 0）'
      } else {
        statusCode = err instanceof ApiError ? err.status : 500
        found = false
        message = err instanceof Error ? err.message : '请求失败'
      }
    }
    const after = await fetchSystemStatus()
    systemStatus.value = after
    probeResult.value = {
      symbol,
      statusCode,
      found,
      dbQueriesDelta: Math.max(0, after.dbQueryCount - before.dbQueryCount),
      dbQueriesTotal: after.dbQueryCount,
      message,
    }
  } finally {
    probing.value = false
  }
}

async function handleToggleSimulator() {
  busyAction.value = true
  try {
    if (systemStatus.value?.simulatorRunning) {
      await stopSimulator()
    } else {
      await startSimulator(systemStatus.value?.simulatorIntervalMs)
    }
    await refreshSystemStatus()
  } finally {
    busyAction.value = false
  }
}

async function handleSetRate(intervalMs: number) {
  busyAction.value = true
  try {
    await updateSimulatorRate(intervalMs)
    if (!systemStatus.value?.simulatorRunning) {
      await startSimulator(intervalMs)
    }
    await refreshSystemStatus()
  } finally {
    busyAction.value = false
  }
}

async function handleEmitOnce() {
  busyAction.value = true
  try {
    await emitSimulatorOnce()
    await Promise.all([refreshSystemStatus(), refreshQuoteAndRules()])
  } finally {
    busyAction.value = false
  }
}

async function handleCrashStorm() {
  busyAction.value = true
  try {
    const res = await triggerCrashStorm(0.05)
    showToast(`已注入全市场暴跌 5% 告警风暴（${res.emitted} 只股票）`)
    await Promise.all([refreshSystemStatus(), refreshQuoteAndRules(), refreshNotifications()])
  } finally {
    busyAction.value = false
  }
}

async function handleCreateRule(payload: { direction: Direction; thresholdPrice: number }) {
  submittingRule.value = true
  ruleError.value = null
  try {
    await createRule({
      userId: selectedUserId.value,
      symbol: selectedSymbol.value,
      direction: payload.direction,
      thresholdPrice: payload.thresholdPrice,
    })
    await refreshQuoteAndRules()
  } catch (err) {
    ruleError.value = err instanceof ApiError ? `[${err.status}] ${err.message}` : String(err)
  } finally {
    submittingRule.value = false
  }
}

async function handleUpdateRule(payload: {
  id: number
  direction: Direction
  thresholdPrice: number
  version: number
}) {
  submittingRule.value = true
  ruleError.value = null
  try {
    await updateRule(payload.id, {
      direction: payload.direction,
      thresholdPrice: payload.thresholdPrice,
      version: payload.version,
    })
    await refreshQuoteAndRules()
  } catch (err) {
    ruleError.value = err instanceof ApiError ? `[${err.status}] ${err.message}` : String(err)
    await refreshQuoteAndRules()
  } finally {
    submittingRule.value = false
  }
}

async function handleDeleteRule(payload: { id: number; version: number }) {
  submittingRule.value = true
  ruleError.value = null
  try {
    await deleteRule(payload.id, payload.version)
    await refreshQuoteAndRules()
  } catch (err) {
    ruleError.value = err instanceof ApiError ? `[${err.status}] ${err.message}` : String(err)
    await refreshQuoteAndRules()
  } finally {
    submittingRule.value = false
  }
}

async function handleMarkRead(id: number) {
  await markNotificationRead(id)
  const target = notifications.value.find((n) => n.id === id)
  if (target && !target.isRead) {
    target.isRead = true
    unreadCount.value = Math.max(0, unreadCount.value - 1)
  }
}

async function handleMarkAllRead() {
  await markAllNotificationsRead(selectedUserId.value)
  notifications.value = notifications.value.map((n) => ({ ...n, isRead: true }))
  unreadCount.value = 0
}

watch(searchQuery, (val) => {
  if (searchTimer != null) {
    window.clearTimeout(searchTimer)
  }
  searchTimer = window.setTimeout(() => {
    void loadStocks(val)
  }, 180)
})

watch(selectedUserId, () => {
  ruleError.value = null
  void refreshQuoteAndRules()
  void refreshNotifications()
  connectWebSocket()
})

onMounted(async () => {
  await loadStocks()
  await Promise.all([
    refreshSystemStatus(),
    refreshQuoteAndRules(),
    refreshKlines(),
    refreshNotifications(),
  ])
  connectWebSocket()

  statusPollTimer = window.setInterval(() => {
    void refreshSystemStatus()
    void refreshQuoteAndRules()
  }, 1000)

  klinePollTimer = window.setInterval(() => {
    void refreshKlines()
  }, 5000)
})

onUnmounted(() => {
  if (statusPollTimer != null) window.clearInterval(statusPollTimer)
  if (klinePollTimer != null) window.clearInterval(klinePollTimer)
  if (searchTimer != null) window.clearTimeout(searchTimer)
  if (reconnectTimer != null) window.clearTimeout(reconnectTimer)
  if (toastTimer != null) window.clearTimeout(toastTimer)
  if (ws) {
    ws.onclose = null
    ws.close()
    ws = null
  }
})
</script>

<template>
  <div class="app-shell">
    <TopConsoleBar
      :status="systemStatus"
      :tps="liveTps"
      v-model:selectedUserId="selectedUserId"
      :users="PRESET_USERS"
      :busyAction="busyAction"
      @toggleSimulator="handleToggleSimulator"
      @setRate="handleSetRate"
      @emitOnce="handleEmitOnce"
      @crashStorm="handleCrashStorm"
    />

    <main class="main-grid">
      <StockSidebar
        :stocks="stocks"
        :selectedSymbol="selectedSymbol"
        v-model:searchQuery="searchQuery"
        :probeResult="probeResult"
        :probing="probing"
        @selectStock="handleSelectStock"
        @probeSymbol="handleProbeSymbol"
      />

      <div class="center-column">
        <KlineChart
          :symbol="selectedSymbol"
          :stockName="selectedStockName"
          :quote="currentQuote"
          :bars="currentKlines"
          :activeRules="activeRules"
        />

        <RulePanel
          :symbol="selectedSymbol"
          :userId="selectedUserId"
          :quote="currentQuote"
          :rules="rules"
          :submitting="submittingRule"
          :errorMessage="ruleError"
          @createRule="handleCreateRule"
          @updateRule="handleUpdateRule"
          @deleteRule="handleDeleteRule"
          @clearError="ruleError = null"
        />
      </div>

      <NotificationCenter
        :notifications="notifications"
        :unreadCount="unreadCount"
        :wsConnected="wsConnected"
        :userId="selectedUserId"
        @markRead="handleMarkRead"
        @markAllRead="handleMarkAllRead"
      />
    </main>

    <div v-if="toastMessage" class="toast-banner">
      {{ toastMessage }}
    </div>
  </div>
</template>
