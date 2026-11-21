export type Direction = 'ABOVE' | 'BELOW'

export type RuleStatus = 'ACTIVE' | 'COMPLETED' | 'DELETED'

export interface Stock {
  id: number
  symbol: string
  name: string
  status: string
}

export interface QuoteSnapshot {
  stockId: number
  symbol: string
  price: number
  priceCents: number
  eventTs: number
  seq: number
}

export interface KlineBar {
  stockId: number
  minuteTs: string
  openPrice: number
  highPrice: number
  lowPrice: number
  closePrice: number
}

export interface AlertRule {
  id: number
  userId: number
  stockId: number
  direction: Direction
  thresholdPrice: number
  status: RuleStatus
  version: number
  createdAt: string
  updatedAt: string
}

export interface NotificationItem {
  id: number
  triggerId: number
  userId: number
  stockId: number
  content: string
  isRead: boolean
  createdAt: string
}

export interface WsNotificationMessage {
  id: number
  triggerId: number
  userId: number
  stockId: number
  symbol: string
  content: string
  isRead: boolean
  createdAt: string | null
}

export interface SystemStatus {
  matchingMode: 'REDIS' | 'DB_FALLBACK' | string
  redisIndexReady: boolean
  circuitBreakerState: string
  lanes: number
  simulatorRunning: boolean
  simulatorIntervalMs: number
  processedQuotes: number
  outOfOrderDiscarded: number
  droppedQuotes: number
  redisMatchCount: number
  fallbackMatchCount: number
  flushedKlineBars: number
  dbQueryCount: number
  maintainerQueueSize: number
  outboxPendingCount: number
  outboxPublishedCount: number
  outboxRetryCount: number
  outboxStaleLeaseCount: number
  triggeredRuleCount: number
  optimisticConflictCount: number
  consumedNotificationCount: number
  duplicateNotificationCount: number
  wsActiveSessions: number
}

export interface PresetUser {
  id: number
  label: string
}

export const PRESET_USERS: PresetUser[] = [
  { id: 1, label: 'User 1 · Alice' },
  { id: 2, label: 'User 2 · Bob' },
  { id: 3, label: 'User 3 · Charlie' },
]
