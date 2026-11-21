<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import type { AlertRule, KlineBar, QuoteSnapshot } from '../types'

const props = defineProps<{
  symbol: string
  stockName: string
  quote: QuoteSnapshot | null
  bars: KlineBar[]
  activeRules: AlertRule[]
}>()

// Live rolling candle map keyed by symbol -> minuteTs -> KlineBar so chart reflects ticks before minute seals
const liveCandleBySymbol = ref<Record<string, KlineBar>>({})
const hoverIndex = ref<number | null>(null)

function toUtcMinuteIso(eventTs: number): string {
  const minuteEpoch = Math.floor(eventTs / 60_000) * 60_000
  return new Date(minuteEpoch).toISOString().slice(0, 19)
}

watch(
  () => props.quote,
  (q) => {
    if (!q || q.eventTs <= 0) return
    const minuteTs = toUtcMinuteIso(q.eventTs)
    const existing = liveCandleBySymbol.value[q.symbol]
    if (!existing || minuteTs > existing.minuteTs) {
      liveCandleBySymbol.value = {
        ...liveCandleBySymbol.value,
        [q.symbol]: {
          stockId: q.stockId,
          minuteTs,
          openPrice: q.price,
          highPrice: q.price,
          lowPrice: q.price,
          closePrice: q.price,
        },
      }
    } else if (minuteTs === existing.minuteTs) {
      liveCandleBySymbol.value = {
        ...liveCandleBySymbol.value,
        [q.symbol]: {
          ...existing,
          highPrice: Math.max(existing.highPrice, q.price),
          lowPrice: Math.min(existing.lowPrice, q.price),
          closePrice: q.price,
        },
      }
    }
  },
  { immediate: true },
)

const displayBars = computed<KlineBar[]>(() => {
  const merged = [...props.bars]
  const live = liveCandleBySymbol.value[props.symbol]
  if (live) {
    const idx = merged.findIndex((b) => b.minuteTs === live.minuteTs)
    if (idx >= 0) {
      const dbBar = merged[idx]
      merged[idx] = {
        ...dbBar,
        highPrice: Math.max(dbBar.highPrice, live.highPrice),
        lowPrice: Math.min(dbBar.lowPrice, live.lowPrice),
        closePrice: live.closePrice,
      }
    } else if (merged.length === 0 || live.minuteTs > merged[merged.length - 1].minuteTs) {
      merged.push(live)
    }
  }
  return merged.slice(-45)
})

const WIDTH = 760
const HEIGHT = 260
const PAD = { top: 18, right: 74, bottom: 26, left: 16 }
const PLOT_W = WIDTH - PAD.left - PAD.right
const PLOT_H = HEIGHT - PAD.top - PAD.bottom

const priceBounds = computed(() => {
  const bars = displayBars.value
  if (bars.length === 0) {
    return { min: 90, max: 110 }
  }
  let min = Math.min(...bars.map((b) => b.lowPrice))
  let max = Math.max(...bars.map((b) => b.highPrice))

  // Include active rules on Y scale when reasonably close so reference lines are visible without flattening candles
  const span = Math.max(max - min, max * 0.01)
  for (const rule of props.activeRules) {
    if (rule.thresholdPrice >= min - span * 1.5 && rule.thresholdPrice <= max + span * 1.5) {
      min = Math.min(min, rule.thresholdPrice)
      max = Math.max(max, rule.thresholdPrice)
    }
  }

  const padding = Math.max((max - min) * 0.14, 0.5)
  return {
    min: Math.max(0.01, min - padding),
    max: max + padding,
  }
})

function yForPrice(price: number): number {
  const { min, max } = priceBounds.value
  const ratio = (price - min) / Math.max(max - min, 0.0001)
  return PAD.top + PLOT_H * (1 - ratio)
}

const yTicks = computed(() => {
  const { min, max } = priceBounds.value
  const steps = 4
  const ticks = []
  for (let i = 0; i <= steps; i++) {
    const val = min + ((max - min) * i) / steps
    ticks.push({
      price: val,
      y: yForPrice(val),
    })
  }
  return ticks
})

const candles = computed(() => {
  const bars = displayBars.value
  const count = Math.max(bars.length, 1)
  const slotW = PLOT_W / Math.max(count, 12)
  const bodyW = Math.max(4, Math.min(18, slotW * 0.62))

  return bars.map((bar, index) => {
    const xCenter = PAD.left + slotW * index + slotW / 2
    const yHigh = yForPrice(bar.highPrice)
    const yLow = yForPrice(bar.lowPrice)
    const yOpen = yForPrice(bar.openPrice)
    const yClose = yForPrice(bar.closePrice)
    const up = bar.closePrice >= bar.openPrice
    return {
      bar,
      index,
      xCenter,
      bodyX: xCenter - bodyW / 2,
      bodyW,
      bodyY: Math.min(yOpen, yClose),
      bodyH: Math.max(2, Math.abs(yClose - yOpen)),
      yHigh,
      yLow,
      color: up ? '#10b981' : '#f43f5e',
      timeLabel: formatTime(bar.minuteTs),
    }
  })
})

const visibleRuleLines = computed(() => {
  const { min, max } = priceBounds.value
  return props.activeRules
    .filter((r) => r.thresholdPrice >= min && r.thresholdPrice <= max)
    .map((r) => ({
      id: r.id,
      direction: r.direction,
      price: r.thresholdPrice,
      y: yForPrice(r.thresholdPrice),
      color: r.direction === 'ABOVE' ? '#10b981' : '#f43f5e',
    }))
})

const offScaleRules = computed(() => {
  const { min, max } = priceBounds.value
  return props.activeRules.filter((r) => r.thresholdPrice < min || r.thresholdPrice > max)
})

const hoveredCandle = computed(() => {
  if (hoverIndex.value == null) return null
  return candles.value[hoverIndex.value] ?? null
})

const latestBar = computed(() => {
  const list = displayBars.value
  return list.length > 0 ? list[list.length - 1] : null
})

function formatTime(minuteTs: string): string {
  const idx = minuteTs.indexOf('T')
  if (idx >= 0) {
    return minuteTs.slice(idx + 1, idx + 6)
  }
  return minuteTs.slice(0, 5)
}
</script>

<template>
  <section class="panel chart-panel">
    <div class="chart-header">
      <div>
        <div class="symbol-line">
          <span class="symbol-code mono">{{ props.symbol }}</span>
          <span class="stock-fullname">{{ props.stockName }}</span>
        </div>
        <div class="ohlc-summary mono" v-if="hoveredCandle">
          <span>{{ hoveredCandle.bar.minuteTs.replace('T', ' ') }} UTC</span>
          <span>O {{ hoveredCandle.bar.openPrice.toFixed(2) }}</span>
          <span>H {{ hoveredCandle.bar.highPrice.toFixed(2) }}</span>
          <span>L {{ hoveredCandle.bar.lowPrice.toFixed(2) }}</span>
          <span>C {{ hoveredCandle.bar.closePrice.toFixed(2) }}</span>
        </div>
        <div class="ohlc-summary mono" v-else-if="latestBar">
          <span>1m K线 ({{ displayBars.length }} 根)</span>
          <span>O {{ latestBar.openPrice.toFixed(2) }}</span>
          <span>H {{ latestBar.highPrice.toFixed(2) }}</span>
          <span>L {{ latestBar.lowPrice.toFixed(2) }}</span>
          <span>C {{ latestBar.closePrice.toFixed(2) }}</span>
        </div>
        <div class="ohlc-summary" v-else>等待首个行情 Tick 更新最新价与 K 线...</div>
      </div>

      <div class="price-box">
        <div v-if="props.quote" class="live-price mono">
          ${{ props.quote.price.toFixed(2) }}
        </div>
        <div v-else class="live-price-empty">暂无行情</div>
        <div v-if="props.quote" class="live-seq mono">
          seq #{{ props.quote.seq }}
        </div>
      </div>
    </div>

    <div v-if="offScaleRules.length > 0" class="offscale-tags">
      <span
        v-for="r in offScaleRules"
        :key="r.id"
        class="badge mono"
        :class="r.direction === 'ABOVE' ? 'badge-emerald' : 'badge-rose'"
      >
        {{ r.direction === 'ABOVE' ? '▲ 涨破' : '▼ 跌破' }} ${{ r.thresholdPrice.toFixed(2) }}
      </span>
    </div>

    <div v-if="candles.length > 0" class="svg-wrap" @mouseleave="hoverIndex = null">
      <svg :viewBox="`0 0 ${WIDTH} ${HEIGHT}`" class="kline-svg" preserveAspectRatio="none">
        <!-- Horizontal Grid + Y Labels -->
        <g v-for="(tick, idx) in yTicks" :key="idx">
          <line
            :x1="PAD.left"
            :x2="WIDTH - PAD.right"
            :y1="tick.y"
            :y2="tick.y"
            stroke="#1e2b40"
            stroke-dasharray="3 3"
          />
          <text
            :x="WIDTH - PAD.right + 6"
            :y="tick.y + 4"
            fill="#64748b"
            font-size="11"
            class="mono"
          >
            {{ tick.price.toFixed(2) }}
          </text>
        </g>

        <!-- Active Rule Threshold Reference Lines -->
        <g v-for="line in visibleRuleLines" :key="'rule-' + line.id">
          <line
            :x1="PAD.left"
            :x2="WIDTH - PAD.right"
            :y1="line.y"
            :y2="line.y"
            :stroke="line.color"
            stroke-width="1.2"
            stroke-dasharray="5 4"
            opacity="0.85"
          />
          <rect
            :x="WIDTH - PAD.right + 2"
            :y="line.y - 9"
            width="66"
            height="18"
            rx="4"
            :fill="line.color"
          />
          <text
            :x="WIDTH - PAD.right + 35"
            :y="line.y + 3"
            text-anchor="middle"
            fill="#0b1120"
            font-size="10"
            font-weight="700"
            class="mono"
          >
            {{ line.price.toFixed(2) }}
          </text>
        </g>

        <!-- Hover Crosshair -->
        <line
          v-if="hoveredCandle"
          :x1="hoveredCandle.xCenter"
          :x2="hoveredCandle.xCenter"
          :y1="PAD.top"
          :y2="HEIGHT - PAD.bottom"
          stroke="#38bdf8"
          stroke-dasharray="3 3"
          opacity="0.6"
        />

        <!-- Candlesticks -->
        <g
          v-for="c in candles"
          :key="c.bar.minuteTs"
          @mouseenter="hoverIndex = c.index"
        >
          <rect
            :x="c.bodyX - 4"
            :y="PAD.top"
            :width="c.bodyW + 8"
            :height="PLOT_H"
            fill="transparent"
          />
          <line
            :x1="c.xCenter"
            :x2="c.xCenter"
            :y1="c.yHigh"
            :y2="c.yLow"
            :stroke="c.color"
            stroke-width="1.4"
          />
          <rect
            :x="c.bodyX"
            :y="c.bodyY"
            :width="c.bodyW"
            :height="c.bodyH"
            :fill="c.color"
            rx="1"
          />
          <text
            v-if="c.index === 0 || c.index === candles.length - 1 || c.index % 6 === 0"
            :x="c.xCenter"
            :y="HEIGHT - 6"
            text-anchor="middle"
            fill="#64748b"
            font-size="10"
            class="mono"
          >
            {{ c.timeLabel }}
          </text>
        </g>
      </svg>
    </div>

    <div v-else class="empty-chart">
      点击顶部「启动行情流」或「单次发 100 股」，即可实时生成 {{ props.symbol }} 价格与 1 分钟 K 线。
    </div>
  </section>
</template>

<style scoped>
.chart-panel {
  padding: 14px 16px;
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.chart-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
}

.symbol-line {
  display: flex;
  align-items: baseline;
  gap: 8px;
}

.symbol-code {
  font-size: 20px;
  font-weight: 800;
  color: var(--text-primary);
}

.stock-fullname {
  color: var(--text-secondary);
  font-size: 13px;
}

.ohlc-summary {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  margin-top: 4px;
  font-size: 12px;
  color: var(--text-muted);
}

.price-box {
  text-align: right;
}

.live-price {
  font-size: 24px;
  font-weight: 800;
  color: var(--accent-sky);
}

.live-price-empty {
  font-size: 14px;
  color: var(--text-muted);
}

.live-seq {
  font-size: 11px;
  color: var(--text-muted);
}

.offscale-tags {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}

.svg-wrap {
  width: 100%;
  background: var(--bg-canvas);
  border: 1px solid var(--border-subtle);
  border-radius: 8px;
  overflow: hidden;
}

.kline-svg {
  display: block;
  width: 100%;
  height: 260px;
}

.empty-chart {
  height: 220px;
  border-radius: 8px;
  border: 1px dashed var(--border-strong);
  background: var(--bg-canvas);
  display: flex;
  align-items: center;
  justify-content: center;
  color: var(--text-muted);
  text-align: center;
  padding: 16px;
}
</style>
