package com.github.lechandonga.embeddedmessagebrokerjava.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class MessageBrokerTest {

    private static final String TOPIC = "orders";
    private static final String GROUP = "g1";

    private MessageBroker newBroker(MutableClock clock) {
        BrokerConfig config = BrokerConfig.builder()
                .topicCapacity(1000)
                .maxRetries(2)
                .ackTimeout(Duration.ofSeconds(30))
                .baseBackoff(Duration.ofMillis(100))
                .maxBackoff(Duration.ofSeconds(5))
                .build();
        return new MessageBroker(config, clock);
    }

    @Nested
    @DisplayName("业务1: 并发投递与拉取")
    class Concurrency {

        @Test
        void concurrentPublishAndConsume_noLossNoDuplicate_fifoPerPartition() throws Exception {
            MessageBroker broker = newBroker(new MutableClock(Instant.now()));
            int producers = 8, perProducer = 50, partitions = 4, consumers = 4;
            int total = producers * perProducer;
            ExecutorService pool = Executors.newFixedThreadPool(producers + consumers);
            CountDownLatch published = new CountDownLatch(total);
            Set<String> receivedIds = ConcurrentHashMap.newKeySet();
            Map<String, List<Long>> offsetsByConsumerAndPartition = new ConcurrentHashMap<>();
            AtomicInteger consumed = new AtomicInteger();

            for (int p = 0; p < producers; p++) {
                int producer = p;
                pool.submit(() -> {
                    for (int i = 0; i < perProducer; i++) {
                        broker.publish(TOPIC, "pk-" + (i % partitions),
                                "payload-" + producer + "-" + i, "dedup-" + producer + "-" + i);
                        published.countDown();
                    }
                });
            }
            for (int c = 0; c < consumers; c++) {
                int consumer = c;
                pool.submit(() -> {
                    while (consumed.get() < total) {
                        List<Message> batch = broker.poll(GROUP, "c" + consumer, TOPIC, 10);
                        for (Message m : batch) {
                            broker.ack(GROUP, "c" + consumer, m.id());
                            receivedIds.add(m.id());
                            offsetsByConsumerAndPartition
                                    .computeIfAbsent(consumer + m.partitionKey(), k -> new ArrayList<>())
                                    .add(m.offset());
                            consumed.incrementAndGet();
                        }
                    }
                });
            }
            assertTrue(published.await(30, TimeUnit.SECONDS));
            pool.shutdown();
            assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS));

            assertEquals(total, consumed.get(), "并发消费总数必须等于投递总数，不能丢消息");
            assertEquals(total, receivedIds.size(), "同一条消息不能交给两个消费者");
            for (List<Long> offsets : offsetsByConsumerAndPartition.values()) {
                for (int i = 1; i < offsets.size(); i++) {
                    assertTrue(offsets.get(i) > offsets.get(i - 1), "同一分区内必须保持 FIFO");
                }
            }
            TestLog.pass("并发投递与拉取", "8生产者x50条/4消费者并发",
                    "总数=" + consumed.get() + "，无丢失、无重复投递、分区内FIFO");
        }
    }

    @Nested
    @DisplayName("业务2: 确认与位点提交")
    class OffsetCommit {

        @Test
        void commitAdvancesOnlyAfterAck_andRejectsRegressionAndGap() {
            MessageBroker broker = newBroker(new MutableClock(Instant.now()));
            Message m0 = broker.publish(TOPIC, "pk", "a", null);
            Message m1 = broker.publish(TOPIC, "pk", "b", null);
            broker.publish(TOPIC, "pk", "c", null);

            BrokerException gap = assertThrows(BrokerException.class,
                    () -> broker.commitOffset(GROUP, TOPIC, "pk", 0));
            assertEquals(ErrorCode.OFFSET_GAP, gap.getCode());

            for (Message m : broker.poll(GROUP, "c1", TOPIC, 2)) {
                broker.ack(GROUP, "c1", m.id());
            }
            broker.commitOffset(GROUP, TOPIC, "pk", m1.offset());
            assertEquals(m1.offset(), broker.committedOffset(GROUP, TOPIC, "pk"));

            BrokerException regression = assertThrows(BrokerException.class,
                    () -> broker.commitOffset(GROUP, TOPIC, "pk", m0.offset()));
            assertEquals(ErrorCode.OFFSET_REGRESSION, regression.getCode());

            BrokerException stillGap = assertThrows(BrokerException.class,
                    () -> broker.commitOffset(GROUP, TOPIC, "pk", m1.offset() + 1));
            assertEquals(ErrorCode.OFFSET_GAP, stillGap.getCode());

            broker.commitOffset(GROUP, TOPIC, "pk", m1.offset());
            assertEquals(m1.offset(), broker.committedOffset(GROUP, TOPIC, "pk"));
            TestLog.pass("确认与位点提交", "位点单调推进/回退拒绝/跳变拒绝",
                    "OFFSET_REGRESSION 与 OFFSET_GAP 可区分");
        }

        @Test
        void ackRequiresValidLease() {
            MessageBroker broker = newBroker(new MutableClock(Instant.now()));
            broker.publish(TOPIC, "pk", "a", null);
            Message m = broker.poll(GROUP, "c1", TOPIC, 1).get(0);

            BrokerException wrongConsumer = assertThrows(BrokerException.class,
                    () -> broker.ack(GROUP, "c2", m.id()));
            assertEquals(ErrorCode.NOT_LEASED, wrongConsumer.getCode());

            broker.ack(GROUP, "c1", m.id());
            BrokerException missing = assertThrows(BrokerException.class,
                    () -> broker.ack(GROUP, "c1", "not-exist"));
            assertEquals(ErrorCode.MESSAGE_NOT_FOUND, missing.getCode());
            TestLog.pass("确认与位点提交", "确认必须持有有效租约", "非租约持有者确认被拒绝");
        }
    }

    @Nested
    @DisplayName("业务3: 超时重投与顺序保持")
    class Redelivery {

        @Test
        void timeoutRedeliveryKeepsPartitionOrder() {
            MutableClock clock = new MutableClock(Instant.now());
            MessageBroker broker = newBroker(clock);
            broker.publish(TOPIC, "pk", "m0", null);
            broker.publish(TOPIC, "pk", "m1", null);

            List<Message> first = broker.poll(GROUP, "c1", TOPIC, 2);
            assertEquals(2, first.size());
            assertTrue(broker.poll(GROUP, "c2", TOPIC, 2).isEmpty(), "租约期内对其他消费者不可见");

            clock.advance(Duration.ofSeconds(31));
            assertTrue(broker.poll(GROUP, "c2", TOPIC, 2).isEmpty(), "退避窗口内不可见");

            clock.advance(Duration.ofMillis(100));
            List<Message> redelivered = broker.poll(GROUP, "c2", TOPIC, 2);
            assertEquals(List.of("m0", "m1"),
                    redelivered.stream().map(Message::payload).toList(),
                    "超时重投不能打乱分区内顺序");
            TestLog.pass("超时重投与顺序保持", "租约超时后由其他消费者按原顺序重投",
                    "时钟注入，无需真实等待");
        }
    }

    @Nested
    @DisplayName("业务4: 重试退避与死信重放")
    class RetryAndDeadLetter {

        @Test
        void backoffIsExponentialAndCapped() {
            BrokerConfig config = BrokerConfig.builder()
                    .baseBackoff(Duration.ofMillis(100))
                    .maxBackoff(Duration.ofMillis(250))
                    .build();
            assertEquals(Duration.ofMillis(100), config.backoffFor(1));
            assertEquals(Duration.ofMillis(200), config.backoffFor(2));
            assertEquals(Duration.ofMillis(250), config.backoffFor(3));
            TestLog.pass("重试退避与死信重放", "指数退避并封顶", "100ms -> 200ms -> 250ms(封顶)");
        }

        @Test
        void retriesExhaustedMovesToDeadLetter_andReplayRespectsDedup() {
            MutableClock clock = new MutableClock(Instant.now());
            MessageBroker broker = newBroker(clock);
            broker.publish(TOPIC, "pk", "hot", "dedup-1");

            for (int round = 0; round < 3; round++) {
                List<Message> batch = broker.poll(GROUP, "c1", TOPIC, 1);
                if (batch.isEmpty()) {
                    clock.advance(Duration.ofSeconds(1));
                    batch = broker.poll(GROUP, "c1", TOPIC, 1);
                }
                if (batch.isEmpty()) {
                    break;
                }
                broker.fail(GROUP, "c1", batch.get(0).id());
            }

            List<DeadLetter> deadLetters = broker.deadLetters(TOPIC);
            assertEquals(1, deadLetters.size(), "超过重试上限必须进入死信");
            assertEquals("hot", deadLetters.get(0).message().payload());

            BrokerException dup = assertThrows(BrokerException.class,
                    () -> broker.publish(TOPIC, "pk", "hot-again", "dedup-1"));
            assertEquals(ErrorCode.DUPLICATE_MESSAGE, dup.getCode());

            Message replayed = broker.replay(deadLetters.get(0).deadLetterId());
            assertEquals(0, replayed.attempts(), "重放后重试计数清零");
            List<Message> polled = broker.poll(GROUP, "c1", TOPIC, 1);
            assertEquals(replayed.id(), polled.get(0).id());

            BrokerException stillDup = assertThrows(BrokerException.class,
                    () -> broker.publish(TOPIC, "pk", "hot-again", "dedup-1"));
            assertEquals(ErrorCode.DUPLICATE_MESSAGE, stillDup.getCode(), "重放不能绕过去重");

            BrokerException notFound = assertThrows(BrokerException.class,
                    () -> broker.replay(deadLetters.get(0).deadLetterId()));
            assertEquals(ErrorCode.DEAD_LETTER_NOT_FOUND, notFound.getCode());
            TestLog.pass("重试退避与死信重放", "重试耗尽转死信/查询/重放/重放不绕过去重",
                    "死信可查询可重放，去重键全程生效");
        }
    }

    @Nested
    @DisplayName("业务5: 去重与边界拒绝")
    class DedupAndRejection {

        @Test
        void duplicateDedupKeyTakesEffectOnlyOnce() {
            MessageBroker broker = newBroker(new MutableClock(Instant.now()));
            broker.publish(TOPIC, "pk", "a", "k1");
            BrokerException dup = assertThrows(BrokerException.class,
                    () -> broker.publish(TOPIC, "pk", "a-copy", "k1"));
            assertEquals(ErrorCode.DUPLICATE_MESSAGE, dup.getCode());
            assertEquals(1, broker.poll(GROUP, "c1", TOPIC, 10).size(), "重复投递只能生效一次");
            TestLog.pass("去重与边界拒绝", "相同去重键重复投递被拒绝", "DUPLICATE_MESSAGE");
        }

        @Test
        void capacityExceededIsRejectedNotDropped() {
            BrokerConfig config = BrokerConfig.builder().topicCapacity(2).build();
            MessageBroker broker = new MessageBroker(config, new MutableClock(Instant.now()));
            broker.publish(TOPIC, "pk", "a", null);
            broker.publish(TOPIC, "pk", "b", null);
            BrokerException full = assertThrows(BrokerException.class,
                    () -> broker.publish(TOPIC, "pk", "c", null));
            assertEquals(ErrorCode.CAPACITY_EXCEEDED, full.getCode());
            TestLog.pass("去重与边界拒绝", "容量超限明确拒绝", "CAPACITY_EXCEEDED，不静默丢弃");
        }

        @Test
        void invalidArgumentsAreRejected() {
            MessageBroker broker = newBroker(new MutableClock(Instant.now()));
            assertEquals(ErrorCode.INVALID_ARGUMENT, assertThrows(BrokerException.class,
                    () -> broker.publish(" ", "pk", "a", null)).getCode());
            assertEquals(ErrorCode.INVALID_ARGUMENT, assertThrows(BrokerException.class,
                    () -> broker.publish(TOPIC, "pk", null, null)).getCode());
            assertEquals(ErrorCode.INVALID_ARGUMENT, assertThrows(BrokerException.class,
                    () -> broker.poll(GROUP, "c1", TOPIC, 0)).getCode());
            TestLog.pass("去重与边界拒绝", "非法参数明确拒绝", "空主题/null负载/非正批量");
        }
    }
}
