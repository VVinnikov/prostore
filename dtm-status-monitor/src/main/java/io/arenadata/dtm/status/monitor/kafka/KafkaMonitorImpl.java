package io.arenadata.dtm.status.monitor.kafka;

import io.arenadata.dtm.common.status.kafka.StatusRequest;
import io.arenadata.dtm.common.status.kafka.StatusResponse;
import io.arenadata.dtm.status.monitor.config.AppProperties;
import kafka.common.OffsetAndMetadata;
import kafka.coordinator.group.BaseKey;
import kafka.coordinator.group.GroupMetadataManager;
import kafka.coordinator.group.GroupTopicPartition;
import kafka.coordinator.group.OffsetKey;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Component
@Slf4j
public class KafkaMonitorImpl implements KafkaMonitor {
    private static final String SYSTEM_TOPIC = "__consumer_offsets";
    private static final String CONSUMER_GROUP = "kafka.status.monitor";

    private final KafkaConsumer<byte[], byte[]> offsetProvider;
    private final KafkaConsumer<byte[], byte[]> lastMessageTimeProvider;
    private final KafkaAdminClient adminClient;
    private final AppProperties appProperties;
    private final ExecutorService consumerService;
    private final Properties consumerProperties;

    private final ConcurrentHashMap<TopicPartition, Long> uncommitedOffsets = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<GroupTopicPartition, OffsetAndMetadata> commitedOffsets = new ConcurrentHashMap<>();

    public KafkaMonitorImpl(AppProperties appProperties) {
        this.appProperties = appProperties;

        consumerProperties = getConsumerProperties();

        consumerService = Executors.newFixedThreadPool(appProperties.getConsumersCount());
        IntStream.range(0, appProperties.getConsumersCount()).forEach(i -> consumerService.submit(this::startConsumer));

        offsetProvider = new KafkaConsumer<>(consumerProperties);
        offsetProvider.subscribe(Collections.singletonList(SYSTEM_TOPIC));

        lastMessageTimeProvider = new KafkaConsumer<>(consumerProperties);

        adminClient = (KafkaAdminClient) AdminClient.create(consumerProperties);
    }

    @SneakyThrows
    @Override
    public StatusResponse status(StatusRequest request) {
        return collectInfo(request);
    }

    @Override
    public List<StatusResponse> listAll() {
        return null;
    }

    private StatusResponse collectInfo(StatusRequest request) {
        StatusResponse response = new StatusResponse();
        response.setConsumerGroup(request.getConsumerGroup());
        response.setTopic(request.getTopic());

        // make a local copy of current kafka state
        List<GroupTopicPartition> partitions = commitedOffsets.keySet().stream()
                .filter(p -> p.topicPartition().topic().equals(request.getTopic()) &&
                        p.group().equals(request.getConsumerGroup()))
                .collect(Collectors.toList());

        response.setLastMessageTime(getLastMessageTime(request.getTopic()));

        log.debug("Fetching end offsets");
        updateLatestOffsets(request.getConsumerGroup());
        val endOffsets = uncommitedOffsets.entrySet().stream()
                .filter(e -> e.getKey().topic().equals(request.getTopic()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        endOffsets.forEach((tp, offset) -> response.setProducerOffset(response.getProducerOffset() + offset));
        log.debug(String.format("Finish fetching end offsets, received %d", endOffsets.entrySet().size()));

        partitions.forEach(tp -> {
            OffsetAndMetadata offset = commitedOffsets.get(tp);
            response.setConsumerOffset(offset.offset() + response.getConsumerOffset());
            response.setLastCommitTime(Math.max(offset.commitTimestamp(), response.getLastCommitTime()));
        });

        return response;
    }

    private Properties getConsumerProperties() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, appProperties.getBrokersList());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP + UUID.randomUUID().toString());
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        return props;
    }

    private void startConsumer() {
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Collections.singletonList(SYSTEM_TOPIC));

        while (true) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<byte[], byte[]> record : records) {
                try {
                    updateOffsets(record);
                } catch (Exception e) {
                    log.error("Error parse message", e);
                }
            }
        }
    }

    @SneakyThrows
    private void updateOffsets(ConsumerRecord<byte[], byte[]> record) {
        byte[] key = record.key();
        byte[] value = record.value();
        if (key == null || value == null) {
            return;
        }

        BaseKey baseKey = GroupMetadataManager.readMessageKey(ByteBuffer.wrap(key));
        if (baseKey instanceof OffsetKey) {
            OffsetKey offsetKey = (OffsetKey) baseKey;
            String topic = offsetKey.key().topicPartition().topic();
            String consumerGroup = offsetKey.key().group();
            int partition = offsetKey.key().topicPartition().partition();

            OffsetAndMetadata offset = GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(value));
            // Because all OffsetKey messages for specified group, topic and partition are placed into one partition,
            // so only one Consumer thread will read and update them.
            // We replay all messages from specified partition in chronological order, and we can perform simple update by key
            commitedOffsets.put(new GroupTopicPartition(consumerGroup, topic, partition), offset);
            log.debug(String.format("Received offset %d for topic %s, partition %d, group %s", offset.offset(),
                    topic,
                    partition,
                    consumerGroup));
        }
    }

    private void updateLatestOffsets(String consumerGroup) {
        try {
            DescribeConsumerGroupsResult describeConsumerGroupsResult = adminClient.describeConsumerGroups(Collections.singletonList(consumerGroup));
            Map<String, ConsumerGroupDescription> consumerGroupDescriptions = describeConsumerGroupsResult.all().get();

            List<TopicPartition> topicPartitions = consumerGroupDescriptions.values().stream()
                    .map(ConsumerGroupDescription::members)
                    .flatMap(Collection::stream)
                    .map(MemberDescription::assignment)
                    .map(MemberAssignment::topicPartitions)
                    .flatMap(Set::stream)
                    .collect(Collectors.toList());

            synchronized (offsetProvider) {
                Map<TopicPartition, Long> topicPartitionLongMap = offsetProvider.endOffsets(topicPartitions);
                uncommitedOffsets.putAll(topicPartitionLongMap);
            }

        } catch (Exception e) {
            log.error("Error updating last offsets for subscribed topic of {} consumer group", consumerGroup, e);
        }
    }

    private long getLastMessageTime(String topic) {
        long lastMessageTime = 0;

        List<TopicPartition> partitionsForRequestTopic = lastMessageTimeProvider.partitionsFor(topic).stream()
                .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
                .collect(Collectors.toList());
        synchronized (lastMessageTimeProvider) {
            lastMessageTimeProvider.assign(partitionsForRequestTopic);
            lastMessageTimeProvider.seekToEnd(partitionsForRequestTopic);

            partitionsForRequestTopic.forEach(tp -> {
                long lastOffset = lastMessageTimeProvider.position(tp);
                if (lastOffset > 0) {
                    lastMessageTimeProvider.seek(tp, lastOffset - 1);
                }
            });

            ConsumerRecords<byte[], byte[]> records = lastMessageTimeProvider.poll(Duration.ofMillis(100));
            for (ConsumerRecord<byte[], byte[]> record : records) {
                lastMessageTime = Math.max(record.timestamp(), lastMessageTime);
            }
        }
        return lastMessageTime;
    }

}
