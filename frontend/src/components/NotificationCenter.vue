<script setup lang="ts">
import type { NotificationItem } from '../types'

const props = defineProps<{
  notifications: NotificationItem[]
  unreadCount: number
  wsConnected: boolean
  userId: number
}>()

const emit = defineEmits<{
  markRead: [id: number]
  markAllRead: []
}>()

function formatCreatedAt(raw: string): string {
  if (!raw) return ''
  return raw.replace('T', ' ').slice(0, 19)
}
</script>

<template>
  <aside class="panel notif-panel right-column">
    <div class="notif-header">
      <div class="notif-title-row">
        <span class="notif-title">站内通知中心</span>
        <span v-if="props.unreadCount > 0" class="unread-pill mono">
          {{ props.unreadCount }}
        </span>
      </div>

      <div class="notif-controls">
        <span class="ws-state" :class="props.wsConnected ? 'ws-ok' : 'ws-wait'">
          <span class="dot" :class="props.wsConnected ? 'dot-emerald' : 'dot-amber'"></span>
          {{ props.wsConnected ? 'WS 实时' : 'WS 重连中' }}
        </span>

        <button
          class="btn btn-sm"
          :disabled="props.unreadCount === 0"
          @click="emit('markAllRead')"
        >
          一键已读
        </button>
      </div>
    </div>

    <div class="notif-list">
      <div
        v-for="item in props.notifications"
        :key="item.id"
        class="notif-item"
        :class="{ unread: !item.isRead }"
        @click="!item.isRead && emit('markRead', item.id)"
      >
        <div class="notif-item-top">
          <span class="notif-dot-wrap">
            <span v-if="!item.isRead" class="dot dot-rose"></span>
            <span class="mono notif-id">#T{{ item.triggerId }}</span>
          </span>
          <span class="notif-time mono">{{ formatCreatedAt(item.createdAt) }}</span>
        </div>
        <div class="notif-content">{{ item.content }}</div>
      </div>

      <div v-if="props.notifications.length === 0" class="empty-notifs">
        User {{ props.userId }} 暂无触发通知。<br />
        创建预警规则后，触发「全市场暴跌 5% 告警风暴」即可实时推送到此。
      </div>
    </div>
  </aside>
</template>

<style scoped>
.notif-panel {
  display: flex;
  flex-direction: column;
  overflow: hidden;
  max-height: calc(100vh - 148px);
}

.notif-header {
  padding: 12px;
  border-bottom: 1px solid var(--border-subtle);
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.notif-title-row {
  display: flex;
  align-items: center;
  gap: 8px;
}

.notif-title {
  font-size: 14px;
  font-weight: 700;
}

.unread-pill {
  background: var(--accent-rose);
  color: #fff;
  border-radius: 999px;
  padding: 1px 7px;
  font-size: 11px;
  font-weight: 700;
}

.notif-controls {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.ws-state {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  font-size: 11px;
  font-weight: 600;
}

.ws-ok {
  color: #34d399;
}

.ws-wait {
  color: #fbbf24;
}

.notif-list {
  flex: 1;
  overflow-y: auto;
  padding: 8px;
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.notif-item {
  background: var(--bg-elevated);
  border: 1px solid var(--border-subtle);
  border-radius: 7px;
  padding: 9px 10px;
  transition: border-color 0.15s ease, background 0.15s ease;
}

.notif-item.unread {
  border-color: rgba(244, 63, 94, 0.45);
  background: rgba(244, 63, 94, 0.08);
  cursor: pointer;
}

.notif-item.unread:hover {
  background: rgba(244, 63, 94, 0.13);
}

.notif-item-top {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 4px;
}

.notif-dot-wrap {
  display: inline-flex;
  align-items: center;
  gap: 6px;
}

.notif-id {
  font-size: 11px;
  color: var(--text-muted);
}

.notif-time {
  font-size: 11px;
  color: var(--text-muted);
}

.notif-content {
  font-size: 12px;
  color: var(--text-primary);
  word-break: break-word;
}

.empty-notifs {
  padding: 28px 12px;
  text-align: center;
  color: var(--text-muted);
  font-size: 12px;
  line-height: 1.6;
}
</style>
