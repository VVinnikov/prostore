package ru.ibs.dtm.status.monitor.kafka;

import kafka.common.OffsetAndMetadata;
import kafka.coordinator.group.BaseKey;
import kafka.coordinator.group.GroupMetadataManager;
import kafka.coordinator.group.GroupTopicPartition;
import kafka.coordinator.group.OffsetKey;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.status.kafka.StatusRequest;
import ru.ibs.dtm.common.status.kafka.StatusResponse;
import ru.ibs.dtm.status.monitor.config.AppProperties;

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
    private final AppProperties appProperties;
    private final ExecutorService consumerService;
    private final Properties consumerProperties;

    private final ConcurrentHashMap<GroupTopicPartition, OffsetAndMetadata> offsets = new ConcurrentHashMap<>();

    public KafkaMonitorImpl(AppProperties appProperties) {
        this.appProperties = appProperties;

        consumerProperties = getConsumerProperties();

        consumerService = Executors.newFixedThreadPool(appProperties.getConsumersCount());
        IntStream.range(0, appProperties.getConsumersCount()).forEach(i -> consumerService.submit(this::startConsumer));

        offsetProvider = new KafkaConsumer<>(consumerProperties);
        offsetProvider.subscribe(Collections.singletonList(SYSTEM_TOPIC));
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
        List<GroupTopicPartition> partitions = offsets.keySet().stream()
                .filter(p -> p.topicPartition().topic().equals(request.getTopic()) &&
                        p.group().equals(request.getConsumerGroup()))
                .collect(Collectors.toList());

        // find end offsets for specified topic partitions
        if (partitions.size() == 0) {
            log.warn(String.format("Cannot find actual information for topic %s, group %s", request.getTopic(), request.getConsumerGroup()));
            return response;
        }
        log.debug(String.format("Filtered %d topic partitions to handle", partitions.size()));

        // set last offsets
        log.debug("Fetching end offsets");
        Map<TopicPartition, Long> endOffsets;
        synchronized (this) {
            endOffsets =
                    offsetProvider.endOffsets(partitions.stream().map(GroupTopicPartition::topicPartition).collect(Collectors.toList()));
        }
        endOffsets.forEach((tp, offset) -> response.setProducerOffset(offset + response.getProducerOffset()));
        log.debug(String.format("Finish fetching end offsets, received %d", endOffsets.entrySet().size()));

        // set current offsets
        partitions.forEach(tp -> {
            OffsetAndMetadata offset = offsets.get(tp);
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
            offsets.put(new GroupTopicPartition(consumerGroup, topic, partition), offset);
            log.debug(String.format("Received offset %d for topic %s, partition %d, group %s", offset.offset(),
                    topic,
                    partition,
                    consumerGroup));
        }
    }

}
