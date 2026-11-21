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
    private Outbox outbox = new Outbox();
    private Mq mq = new Mq();

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

    public Outbox getOutbox() {
        return outbox;
    }

    public void setOutbox(Outbox outbox) {
        this.outbox = outbox;
    }

    public Mq getMq() {
        return mq;
    }

    public void setMq(Mq mq) {
        this.mq = mq;
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

    public static class Outbox {
        private boolean enabled = true;
        private long pollIntervalMs = 200;
        private int batchSize = 50;
        private int leaseSeconds = 30;
        private long initialBackoffSeconds = 1;
        private long maxBackoffSeconds = 30;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public long getPollIntervalMs() {
            return pollIntervalMs;
        }

        public void setPollIntervalMs(long pollIntervalMs) {
            this.pollIntervalMs = pollIntervalMs;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public int getLeaseSeconds() {
            return leaseSeconds;
        }

        public void setLeaseSeconds(int leaseSeconds) {
            this.leaseSeconds = leaseSeconds;
        }

        public long getInitialBackoffSeconds() {
            return initialBackoffSeconds;
        }

        public void setInitialBackoffSeconds(long initialBackoffSeconds) {
            this.initialBackoffSeconds = initialBackoffSeconds;
        }

        public long getMaxBackoffSeconds() {
            return maxBackoffSeconds;
        }

        public void setMaxBackoffSeconds(long maxBackoffSeconds) {
            this.maxBackoffSeconds = maxBackoffSeconds;
        }
    }

    public static class Mq {
        private boolean consumerEnabled = true;
        private String topic = "pricepulse-alert-triggered";
        private String consumerGroup = "pricepulse-v3-alert-consumer";
        private int consumeThreadMin = 4;
        private int consumeThreadMax = 8;
        private int maxReconsumeTimes = 3;
        private int reconsumeDelayLevel = 0;
        private long sendTimeoutMs = 3000;

        public boolean isConsumerEnabled() {
            return consumerEnabled;
        }

        public void setConsumerEnabled(boolean consumerEnabled) {
            this.consumerEnabled = consumerEnabled;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public String getConsumerGroup() {
            return consumerGroup;
        }

        public void setConsumerGroup(String consumerGroup) {
            this.consumerGroup = consumerGroup;
        }

        public int getConsumeThreadMin() {
            return consumeThreadMin;
        }

        public void setConsumeThreadMin(int consumeThreadMin) {
            this.consumeThreadMin = consumeThreadMin;
        }

        public int getConsumeThreadMax() {
            return consumeThreadMax;
        }

        public void setConsumeThreadMax(int consumeThreadMax) {
            this.consumeThreadMax = consumeThreadMax;
        }

        public int getMaxReconsumeTimes() {
            return maxReconsumeTimes;
        }

        public void setMaxReconsumeTimes(int maxReconsumeTimes) {
            this.maxReconsumeTimes = maxReconsumeTimes;
        }

        public int getReconsumeDelayLevel() {
            return reconsumeDelayLevel;
        }

        public void setReconsumeDelayLevel(int reconsumeDelayLevel) {
            this.reconsumeDelayLevel = reconsumeDelayLevel;
        }

        public long getSendTimeoutMs() {
            return sendTimeoutMs;
        }

        public void setSendTimeoutMs(long sendTimeoutMs) {
            this.sendTimeoutMs = sendTimeoutMs;
        }
    }
}
