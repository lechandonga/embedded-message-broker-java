package com.github.lechandonga.embeddedmessagebrokerjava.broker;

import java.time.Duration;
import java.util.Objects;

/**
 * 关键配置项，时间相关参数全部可注入，测试可用极小值避免长时间等待。
 */
public class BrokerConfig {

    private final int topicCapacity;
    private final int maxRetries;
    private final Duration ackTimeout;
    private final Duration baseBackoff;
    private final Duration maxBackoff;

    private BrokerConfig(Builder b) {
        this.topicCapacity = b.topicCapacity;
        this.maxRetries = b.maxRetries;
        this.ackTimeout = b.ackTimeout;
        this.baseBackoff = b.baseBackoff;
        this.maxBackoff = b.maxBackoff;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static BrokerConfig defaults() {
        return builder().build();
    }

    /** 单主题允许的最大活跃（未终结）消息数 */
    public int getTopicCapacity() {
        return topicCapacity;
    }

    /** 最大重试次数，超过后转入死信 */
    public int getMaxRetries() {
        return maxRetries;
    }

    /** 消费者确认超时，超时后消息可被重新投递 */
    public Duration getAckTimeout() {
        return ackTimeout;
    }

    /** 退避基数，第 n 次重试退避 baseBackoff * 2^(n-1)，封顶 maxBackoff */
    public Duration getBaseBackoff() {
        return baseBackoff;
    }

    public Duration getMaxBackoff() {
        return maxBackoff;
    }

    /** 计算第 attempt 次（从 1 开始）重试的退避时长，指数退避 + 封顶 */
    public Duration backoffFor(int attempt) {
        long capped = baseBackoff.toMillis();
        for (int i = 1; i < attempt; i++) {
            capped = Math.min(capped * 2, maxBackoff.toMillis());
        }
        return Duration.ofMillis(Math.min(capped, maxBackoff.toMillis()));
    }

    public static class Builder {
        private int topicCapacity = 10_000;
        private int maxRetries = 3;
        private Duration ackTimeout = Duration.ofSeconds(30);
        private Duration baseBackoff = Duration.ofMillis(100);
        private Duration maxBackoff = Duration.ofSeconds(10);

        public Builder topicCapacity(int topicCapacity) {
            this.topicCapacity = topicCapacity;
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder ackTimeout(Duration ackTimeout) {
            this.ackTimeout = ackTimeout;
            return this;
        }

        public Builder baseBackoff(Duration baseBackoff) {
            this.baseBackoff = baseBackoff;
            return this;
        }

        public Builder maxBackoff(Duration maxBackoff) {
            this.maxBackoff = maxBackoff;
            return this;
        }

        public BrokerConfig build() {
            if (topicCapacity <= 0 || maxRetries < 0) {
                throw new BrokerException(ErrorCode.INVALID_ARGUMENT, "capacity/maxRetries 非法");
            }
            Objects.requireNonNull(ackTimeout);
            Objects.requireNonNull(baseBackoff);
            Objects.requireNonNull(maxBackoff);
            return new BrokerConfig(this);
        }
    }
}
