package com.pricepulse;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "pricepulse")
public class PricePulseProperties {

    private int lanes = 16;
    private int laneQueueCapacity = 1024;
    private boolean backpressureBlocking = true;
    private Simulator simulator = new Simulator();
    private ReadCache readCache = new ReadCache();
    private Matching matching = new Matching();

    public int getLanes() {
        return lanes;
    }

    public void setLanes(int lanes) {
        this.lanes = lanes;
    }

    public int getLaneQueueCapacity() {
        return laneQueueCapacity;
    }

    public void setLaneQueueCapacity(int laneQueueCapacity) {
        this.laneQueueCapacity = laneQueueCapacity;
    }

    public boolean isBackpressureBlocking() {
        return backpressureBlocking;
    }

    public void setBackpressureBlocking(boolean backpressureBlocking) {
        this.backpressureBlocking = backpressureBlocking;
    }

    public Simulator getSimulator() {
        return simulator;
    }

    public void setSimulator(Simulator simulator) {
        this.simulator = simulator;
    }

    public ReadCache getReadCache() {
        return readCache;
    }

    public void setReadCache(ReadCache readCache) {
        this.readCache = readCache;
    }

    public Matching getMatching() {
        return matching;
    }

    public void setMatching(Matching matching) {
        this.matching = matching;
    }

    public static class Simulator {
        private boolean enabled = false;
        private long intervalMs = 200;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public long getIntervalMs() {
            return intervalMs;
        }

        public void setIntervalMs(long intervalMs) {
            this.intervalMs = intervalMs;
        }
    }

    public static class ReadCache {
        private boolean enabled = true;
        private long baseTtlSeconds = 300;
        private long nullTtlSeconds = 30;
        private int jitterMaxSeconds = 60;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public long getBaseTtlSeconds() {
            return baseTtlSeconds;
        }

        public void setBaseTtlSeconds(long baseTtlSeconds) {
            this.baseTtlSeconds = baseTtlSeconds;
        }

        public long getNullTtlSeconds() {
            return nullTtlSeconds;
        }

        public void setNullTtlSeconds(long nullTtlSeconds) {
            this.nullTtlSeconds = nullTtlSeconds;
        }

        public int getJitterMaxSeconds() {
            return jitterMaxSeconds;
        }

        public void setJitterMaxSeconds(int jitterMaxSeconds) {
            this.jitterMaxSeconds = jitterMaxSeconds;
        }
    }

    public static class Matching {
        private boolean forceDbFallback = false;
        private long reconcileIntervalMs = 30_000;
        private long recoveryProbeIntervalMs = 2_000;

        public boolean isForceDbFallback() {
            return forceDbFallback;
        }

        public void setForceDbFallback(boolean forceDbFallback) {
            this.forceDbFallback = forceDbFallback;
        }

        public long getReconcileIntervalMs() {
            return reconcileIntervalMs;
        }

        public void setReconcileIntervalMs(long reconcileIntervalMs) {
            this.reconcileIntervalMs = reconcileIntervalMs;
        }

        public long getRecoveryProbeIntervalMs() {
            return recoveryProbeIntervalMs;
        }

        public void setRecoveryProbeIntervalMs(long recoveryProbeIntervalMs) {
            this.recoveryProbeIntervalMs = recoveryProbeIntervalMs;
        }
    }
}
