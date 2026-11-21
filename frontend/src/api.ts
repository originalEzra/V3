import type {
  AlertRule,
  Direction,
  KlineBar,
  NotificationItem,
  QuoteSnapshot,
  Stock,
  SystemStatus,
} from './types'

export class ApiError extends Error {
  status: number

  constructor(status: number, message: string) {
    super(message)
    this.name = 'ApiError'
    this.status = status
  }
}

async function request<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, {
    headers: {
      'Content-Type': 'application/json',
      ...(init?.headers ?? {}),
    },
    ...init,
  })

  const rawText = await response.text()
  if (!response.ok) {
    let message = response.statusText || `HTTP ${response.status}`
    if (rawText) {
      try {
        const parsed = JSON.parse(rawText) as { message?: string; error?: string }
        message = parsed.message || parsed.error || message
      } catch {
        message = rawText
      }
    }
    throw new ApiError(response.status, message)
  }

  if (!rawText || response.status === 204) {
    return null as T
  }
  return JSON.parse(rawText) as T
}

export function fetchStocks(query?: string): Promise<Stock[]> {
  const params = query && query.trim() ? `?q=${encodeURIComponent(query.trim())}` : ''
  return request<Stock[]>(`/api/stocks${params}`)
}

export function fetchQuote(symbol: string): Promise<QuoteSnapshot | null> {
  return request<QuoteSnapshot | null>(`/api/quotes/${encodeURIComponent(symbol)}`)
}

export function fetchKlines(symbol: string, limit = 60): Promise<KlineBar[]> {
  return request<KlineBar[]>(`/api/klines/${encodeURIComponent(symbol)}?limit=${limit}`)
}

export function fetchSystemStatus(): Promise<SystemStatus> {
  return request<SystemStatus>('/api/system/status')
}

export function startSimulator(intervalMs?: number): Promise<{ running: boolean; intervalMs: number }> {
  const qs = intervalMs != null ? `?intervalMs=${intervalMs}` : ''
  return request(`/api/simulator/start${qs}`, { method: 'POST' })
}

export function stopSimulator(): Promise<{ running: boolean; intervalMs: number }> {
  return request('/api/simulator/stop', { method: 'POST' })
}

export function updateSimulatorRate(intervalMs: number): Promise<{ running: boolean; intervalMs: number }> {
  return request(`/api/simulator/rate?intervalMs=${intervalMs}`, { method: 'PUT' })
}

export function emitSimulatorOnce(): Promise<{ emitted: number }> {
  return request('/api/simulator/emit', { method: 'POST' })
}

export function triggerCrashStorm(dropRatio = 0.05): Promise<{ emitted: number; dropRatio: number }> {
  return request(`/api/simulator/crash-storm?dropRatio=${dropRatio}`, { method: 'POST' })
}

export function fetchRules(userId: number, symbol: string): Promise<AlertRule[]> {
  const params = new URLSearchParams({
    userId: String(userId),
    symbol,
  })
  return request<AlertRule[]>(`/api/rules?${params.toString()}`)
}

export function createRule(payload: {
  userId: number
  symbol: string
  direction: Direction
  thresholdPrice: number
}): Promise<AlertRule> {
  return request<AlertRule>('/api/rules', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
}

export function updateRule(
  id: number,
  payload: {
    direction: Direction
    thresholdPrice: number
    version: number
  },
): Promise<AlertRule> {
  return request<AlertRule>(`/api/rules/${id}`, {
    method: 'PUT',
    body: JSON.stringify(payload),
  })
}

export function deleteRule(id: number, version: number): Promise<void> {
  return request<void>(`/api/rules/${id}?version=${version}`, {
    method: 'DELETE',
  })
}

export function fetchNotifications(userId: number, limit = 50): Promise<NotificationItem[]> {
  const params = new URLSearchParams({
    userId: String(userId),
    limit: String(limit),
  })
  return request<NotificationItem[]>(`/api/notifications?${params.toString()}`)
}

export function fetchUnreadCount(userId: number): Promise<{ userId: number; unreadCount: number }> {
  return request<{ userId: number; unreadCount: number }>(
    `/api/notifications/unread-count?userId=${userId}`,
  )
}

export function markNotificationRead(id: number): Promise<{ updated: number }> {
  return request<{ updated: number }>(`/api/notifications/${id}/read`, {
    method: 'POST',
  })
}

export function markAllNotificationsRead(userId: number): Promise<{ updated: number }> {
  return request<{ updated: number }>(`/api/notifications/read-all?userId=${userId}`, {
    method: 'POST',
  })
}

export function buildNotificationsWsUrl(userId: number): string {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  return `${protocol}//${window.location.host}/ws/notifications?userId=${userId}`
}
