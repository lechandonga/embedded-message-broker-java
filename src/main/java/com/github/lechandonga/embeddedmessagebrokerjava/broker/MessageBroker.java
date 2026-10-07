package com.github.lechandonga.embeddedmessagebrokerjava.broker;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/**
 * 进程内消息队列核心。所有状态都在内存中，不依赖外部服务。
 *
 * <p>语义概览：
 * <ul>
 *   <li>同一 (topic, partitionKey) 内按 offset 严格 FIFO；</li>
 *   <li>poll 以租约方式把消息交给某个消费者，租约期内对其他消费者不可见；</li>
 *   <li>只有 ack 的消息才能推进位点；位点单调向前，回退/跳变提交被拒绝；</li>
 *   <li>租约超时或显式 fail 触发指数退避重试，超过 maxRetries 进入死信；</li>
 *   <li>dedupKey 在主题内只生效一次，死信重放沿用原去重键，不绕过去重。</li>
 * </ul>
 * 所有 public 方法 synchronized，保证并发投递/拉取/确认下不丢消息、不重复投递。
 */
public class MessageBroker {

    private enum Status {READY, LEASED, ACKED}

    private static final class Entry {
        final String id;
        final String topic;
        final String partitionKey;
        final long offset;
        final String payload;
        final String dedupKey;
        int attempts;
        Status status = Status.READY;
        String leaseOwner;
        Instant leaseExpiry;
        Instant visibleAt;

        Entry(String id, String topic, String partitionKey, long offset,
              String payload, String dedupKey, Instant visibleAt) {
            this.id = id;
            this.topic = topic;
            this.partitionKey = partitionKey;
            this.offset = offset;
            this.payload = payload;
            this.dedupKey = dedupKey;
            this.visibleAt = visibleAt;
        }

        Message toMessage() {
            return new Message(id, topic, partitionKey, offset, payload, dedupKey, attempts, visibleAt);
        }
    }

    private static final class Partition {
        final String topic;
        final String partitionKey;
        final TreeMap<Long, Entry> entries = new TreeMap<>();
        long nextOffset;

        Partition(String topic, String partitionKey) {
            this.topic = topic;
            this.partitionKey = partitionKey;
        }
    }

    private final BrokerConfig config;
    private final Clock clock;
    /** (topic, partitionKey) -> 分区 */
    private final Map<String, Partition> partitions = new HashMap<>();
    /** (group, topic, partitionKey) -> 已提交位点 */
    private final Map<String, Long> committedOffsets = new HashMap<>();
    /** topic|dedupKey -> messageId，去重登记，永不移除 */
    private final Map<String, String> dedupIndex = new HashMap<>();
    /** deadLetterId -> 死信 */
    private final Map<String, DeadLetter> deadLetters = new HashMap<>();
    /** topic -> 活跃消息数 */
    private final Map<String, Integer> liveCounts = new HashMap<>();

    public MessageBroker(BrokerConfig config) {
        this(config, Clock.systemUTC());
    }

    public MessageBroker(BrokerConfig config, Clock clock) {
        this.config = config;
        this.clock = clock;
    }

    // ---------------- 投递 ----------------

    public synchronized Message publish(String topic, String partitionKey, String payload, String dedupKey) {
        requireText(topic, "topic");
        requireText(partitionKey, "partitionKey");
        if (payload == null) {
            throw new BrokerException(ErrorCode.INVALID_ARGUMENT, "payload 不能为 null");
        }
        if (dedupKey != null) {
            String key = dedupKey(topic, dedupKey);
            if (dedupIndex.containsKey(key)) {
                throw new BrokerException(ErrorCode.DUPLICATE_MESSAGE,
                        "去重键已生效过: " + dedupKey);
            }
        }
        int live = liveCounts.getOrDefault(topic, 0);
        if (live >= config.getTopicCapacity()) {
            throw new BrokerException(ErrorCode.CAPACITY_EXCEEDED,
                    "主题 " + topic + " 活跃消息数超过容量 " + config.getTopicCapacity());
        }
        Partition partition = partitions.computeIfAbsent(partitionKey(topic, partitionKey),
                k -> new Partition(topic, partitionKey));
        Entry entry = new Entry(UUID.randomUUID().toString(), topic, partitionKey,
                partition.nextOffset++, payload, dedupKey, clock.instant());
        partition.entries.put(entry.offset, entry);
        liveCounts.merge(topic, 1, Integer::sum);
        if (dedupKey != null) {
            dedupIndex.put(dedupKey(topic, dedupKey), entry.id);
        }
        return entry.toMessage();
    }

    // ---------------- 拉取 ----------------

    /**
     * 以租约方式拉取消息。同一时刻一条消息只会租约给一个 (group, consumer)。
     * 只返回位点之后、已可见、未被他人租约的消息，按 offset 升序，保证分区内 FIFO。
     */
    public synchronized List<Message> poll(String group, String consumerId, String topic, int max) {
        requireText(group, "group");
        requireText(consumerId, "consumerId");
        requireText(topic, "topic");
        if (max <= 0) {
            throw new BrokerException(ErrorCode.INVALID_ARGUMENT, "max 必须为正数");
        }
        Instant now = clock.instant();
        sweepExpiredLeases(now);
        List<Message> result = new ArrayList<>();
        for (Partition partition : partitions.values()) {
            if (!partition.topic.equals(topic)) {
                continue;
            }
            long committed = committedOffset(group, topic, partition.partitionKey);
            for (Entry entry : partition.entries.values()) {
                if (result.size() >= max) {
                    return result;
                }
                if (entry.offset <= committed
                        || entry.status != Status.READY || entry.visibleAt.isAfter(now)) {
                    continue;
                }
                entry.status = Status.LEASED;
                entry.leaseOwner = owner(group, consumerId);
                entry.leaseExpiry = now.plus(config.getAckTimeout());
                result.add(entry.toMessage());
            }
        }
        return result;
    }

    // ---------------- 确认与位点 ----------------

    /** 确认消息：必须持有未过期租约，否则拒绝。 */
    public synchronized void ack(String group, String consumerId, String messageId) {
        Entry entry = findLeased(group, consumerId, messageId);
        entry.status = Status.ACKED;
        entry.leaseOwner = null;
        entry.leaseExpiry = null;
    }

    /**
     * 提交位点。只允许推进到“连续已确认”的边界：
     * 回退提交 -> OFFSET_REGRESSION；跳过未确认消息 -> OFFSET_GAP。
     */
    public synchronized void commitOffset(String group, String topic, String partitionKey, long offset) {
        requireText(group, "group");
        requireText(topic, "topic");
        requireText(partitionKey, "partitionKey");
        String pk = partitionKey(topic, partitionKey);
        long committed = committedOffset(group, topic, partitionKey);
        if (offset < committed) {
            throw new BrokerException(ErrorCode.OFFSET_REGRESSION,
                    "位点回退: committed=" + committed + ", target=" + offset);
        }
        if (offset == committed) {
            return;
        }
        Partition partition = partitions.get(pk);
        long frontier = ackedFrontier(partition, committed);
        if (offset > frontier) {
            throw new BrokerException(ErrorCode.OFFSET_GAP,
                    "位点跳过未确认消息: frontier=" + frontier + ", target=" + offset);
        }
        committedOffsets.put(offsetKey(group, topic, partitionKey), offset);
        // 清理已提交且已确认的消息，释放容量
        partition.entries.headMap(offset + 1, true).values().removeIf(e -> {
            boolean done = e.status == Status.ACKED;
            if (done) {
                liveCounts.merge(topic, -1, Integer::sum);
            }
            return done;
        });
    }

    public synchronized long committedOffset(String group, String topic, String partitionKey) {
        return committedOffsets.getOrDefault(offsetKey(group, topic, partitionKey), -1L);
    }

    // ---------------- 失败与重试 ----------------

    /** 显式上报消费失败，立即触发退避重试或转死信。 */
    public synchronized void fail(String group, String consumerId, String messageId) {
        Entry entry = findLeased(group, consumerId, messageId);
        retryOrDeadLetter(entry, clock.instant(), "消费显式失败");
    }

    // ---------------- 死信 ----------------

    public synchronized List<DeadLetter> deadLetters(String topic) {
        requireText(topic, "topic");
        List<DeadLetter> result = new ArrayList<>();
        for (DeadLetter dl : deadLetters.values()) {
            if (dl.message().topic().equals(topic)) {
                result.add(dl);
            }
        }
        return result;
    }

    /**
     * 重放死信：以原去重键重新入队（新消息 id、新 offset、重试计数清零）。
     * 去重登记仍然保留，重放后相同 dedupKey 的投递依旧会被拒绝，不绕过去重规则。
     */
    public synchronized Message replay(String deadLetterId) {
        requireText(deadLetterId, "deadLetterId");
        DeadLetter dl = deadLetters.remove(deadLetterId);
        if (dl == null) {
            throw new BrokerException(ErrorCode.DEAD_LETTER_NOT_FOUND, "死信不存在: " + deadLetterId);
        }
        Message old = dl.message();
        int live = liveCounts.getOrDefault(old.topic(), 0);
        if (live >= config.getTopicCapacity()) {
            deadLetters.put(deadLetterId, dl);
            throw new BrokerException(ErrorCode.CAPACITY_EXCEEDED, "主题容量已满，无法重放");
        }
        Partition partition = partitions.computeIfAbsent(
                partitionKey(old.topic(), old.partitionKey()), k -> new Partition(old.topic(), old.partitionKey()));
        Entry entry = new Entry(UUID.randomUUID().toString(), old.topic(), old.partitionKey(),
                partition.nextOffset++, old.payload(), old.dedupKey(), clock.instant());
        partition.entries.put(entry.offset, entry);
        liveCounts.merge(old.topic(), 1, Integer::sum);
        if (old.dedupKey() != null) {
            dedupIndex.put(dedupKey(old.topic(), old.dedupKey()), entry.id);
        }
        return entry.toMessage();
    }

    // ---------------- 内部 ----------------

    /** 扫描过期租约：按失败处理（退避重试或转死信），保证分区内顺序不被打乱。 */
    private void sweepExpiredLeases(Instant now) {
        for (Partition partition : partitions.values()) {
            List<Entry> expired = new ArrayList<>();
            for (Entry entry : partition.entries.values()) {
                if (entry.status == Status.LEASED && !entry.leaseExpiry.isAfter(now)) {
                    expired.add(entry);
                }
            }
            for (Entry entry : expired) {
                retryOrDeadLetter(entry, now, "确认超时");
            }
        }
    }

    private void retryOrDeadLetter(Entry entry, Instant now, String reason) {
        entry.attempts++;
        entry.leaseOwner = null;
        entry.leaseExpiry = null;
        if (entry.attempts > config.getMaxRetries()) {
            Partition partition = partitions.get(partitionKey(entry.topic, entry.partitionKey));
            partition.entries.remove(entry.offset);
            liveCounts.merge(entry.topic, -1, Integer::sum);
            String deadLetterId = UUID.randomUUID().toString();
            deadLetters.put(deadLetterId, new DeadLetter(deadLetterId, entry.toMessage(),
                    reason + "，重试次数超过上限 " + config.getMaxRetries(), now));
        } else {
            entry.status = Status.READY;
            entry.visibleAt = now.plus(config.backoffFor(entry.attempts));
        }
    }

    private Entry findLeased(String group, String consumerId, String messageId) {
        requireText(group, "group");
        requireText(consumerId, "consumerId");
        requireText(messageId, "messageId");
        for (Partition partition : partitions.values()) {
            for (Entry entry : partition.entries.values()) {
                if (entry.id.equals(messageId)) {
                    String owner = owner(group, consumerId);
                    if (entry.status != Status.LEASED || !owner.equals(entry.leaseOwner)
                            || !entry.leaseExpiry.isAfter(clock.instant())) {
                        throw new BrokerException(ErrorCode.NOT_LEASED,
                                "消息未租约给该消费者或租约已过期: " + messageId);
                    }
                    return entry;
                }
            }
        }
        throw new BrokerException(ErrorCode.MESSAGE_NOT_FOUND, "消息不存在或已终结: " + messageId);
    }

    /** 从 committed 之后连续已确认（或已终结）的最大 offset。 */
    private long ackedFrontier(Partition partition, long committed) {
        long frontier = committed;
        if (partition == null) {
            return frontier;
        }
        for (long offset = committed + 1; offset < partition.nextOffset; offset++) {
            Entry entry = partition.entries.get(offset);
            if (entry == null) {
                frontier = offset; // 已终结（死信或已清理），可跨过
                continue;
            }
            if (entry.status != Status.ACKED) {
                break;
            }
            frontier = offset;
        }
        return frontier;
    }


    private static String partitionKey(String topic, String partitionKey) {
        return topic + '#' + partitionKey;
    }

    private static String offsetKey(String group, String topic, String partitionKey) {
        return group + '#' + topic + '#' + partitionKey;
    }

    private static String dedupKey(String topic, String dedupKey) {
        return topic + '|' + dedupKey;
    }

    private static String owner(String group, String consumerId) {
        return group + ':' + consumerId;
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new BrokerException(ErrorCode.INVALID_ARGUMENT, name + " 不能为空");
        }
    }
}
