package ru.ibs.dtm.kafka.core.service.kafka;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.kafka.admin.ConsumerGroupListing;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.admin.MemberDescription;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import kafka.common.OffsetAndMetadata;
import kafka.coordinator.group.GroupMetadataManager;
import kafka.coordinator.group.GroupTopicPartition;
import kafka.coordinator.group.OffsetKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.plugin.status.kafka.KafkaGroupTopic;
import ru.ibs.dtm.common.plugin.status.kafka.KafkaPartitionInfo;
import ru.ibs.dtm.common.plugin.status.kafka.KafkaTopicCommitedOffset;
import ru.ibs.dtm.common.plugin.status.kafka.KafkaTopicOffset;
import ru.ibs.dtm.kafka.core.configuration.properties.KafkaProperties;
import ru.ibs.dtm.kafka.core.factory.KafkaConsumerFactory;

import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.*;

@Component
@Slf4j
public class KafkaConsumerMonitorImpl implements KafkaConsumerMonitor {

    private static final String SYSTEM_TOPIC = "__consumer_offsets";
    private final KafkaAdminClient adminClient;
    private final KafkaConsumer<Buffer, Buffer> consumer;
    private final Vertx vertx;
    private final ConcurrentHashMap<GroupTopicPartition, KafkaTopicCommitedOffset> lastCommitedOffsets;
    private final ConcurrentHashMap<GroupTopicPartition, KafkaTopicOffset> lastOffsets;
    private final KafkaProperties kafkaProperties;


    @Autowired
    public KafkaConsumerMonitorImpl(@Qualifier("coreKafkaAdminClient") KafkaAdminClient adminClient,
                                    @Qualifier("coreKafkaConsumerFactory") KafkaConsumerFactory<Buffer, Buffer> consumerFactory,
                                    @Qualifier("kafkaVertx") Vertx vertx,
                                    KafkaProperties kafkaProperties) {

        this.kafkaProperties = kafkaProperties;
        this.adminClient = adminClient;

        // Set Properties
        HashMap<String, String> properties = new HashMap<>();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "monitoring_consumer_" + UUID.randomUUID().toString());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        this.consumer = consumerFactory.create(properties);

        // VertX
        this.vertx = vertx;

        // State
        lastCommitedOffsets = new ConcurrentHashMap<>();
        lastOffsets = new ConcurrentHashMap<>();
        initSystemTopicConsumer();
        initTopicOffsetRefresh();
    }

    private Future<List<String>> getConsumerGroupNames() {
        return Future.future(handler ->
                adminClient.listConsumerGroups(ar -> {
                    if (ar.succeeded()) {
                        List<String> consumerGroupNames = ar.result().stream()
                                .map(ConsumerGroupListing::getGroupId).collect(toList());
                        handler.complete(consumerGroupNames);

                    } else {
                        log.error("Can't get consumers group list", ar.cause());
                        handler.fail(ar.cause());
                    }
                }));
    }

    private Future<Map<String, Set<TopicPartition>>> getConsumerGroupTopicPartitions(List<String> consumerGroupNames) {
        return Future.future(handler ->
                adminClient.describeConsumerGroups(consumerGroupNames, ar -> {
                    if (ar.succeeded()) {
                        Map<String, Set<TopicPartition>> topicPartitions = ar.result().entrySet().stream()
                                .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                e -> e.getValue().getMembers().stream().map(MemberDescription::getAssignment)
                                        .flatMap(assignment -> assignment.getTopicPartitions().stream())
                                        .collect(toSet())
                        ));
                        handler.complete(topicPartitions);
                    } else {
                        log.error("Can't get topic partition info", ar.cause());
                        handler.fail(ar.cause());
                    }
                }));
    }


    private Future<Map<TopicPartition, Long>> getBeginOffset(Set<TopicPartition> topicPartitions) {
        return Future.future(handler -> consumer.beginningOffsets(topicPartitions, ar -> {
            if (ar.succeeded())
                handler.complete(ar.result());
            else {
                log.error("Can't get topic begin offset", ar.cause());
                handler.fail(ar.cause());
            }
        }));
    }

    private Future<Map<TopicPartition, Long>> getEndOffset(Set<TopicPartition> topicPartitions) {
        return Future.future(handler -> consumer.endOffsets(topicPartitions, ar -> {
            if (ar.succeeded())
                handler.complete(ar.result());
            else {
                log.error("Can't get topic end offset", ar.cause());
                handler.fail(ar.cause());
            }
        }));
    }

    private Future<Map<TopicPartition, KafkaTopicOffset>> getBeginEndOffsets(Set<TopicPartition> topicPartitions) {
        Future<Map<TopicPartition, Long>> beginOffsetFuture = getBeginOffset(topicPartitions);
        Future<Map<TopicPartition, Long>> endOffsetFuture = getEndOffset(topicPartitions);

        return Future.future(handler -> CompositeFuture.all(beginOffsetFuture, endOffsetFuture).setHandler(ar -> {
            if (ar.succeeded()) {
                Map<TopicPartition, Long> beginOffset = ar.result().resultAt(0);
                Map<TopicPartition, Long> endOffset = ar.result().resultAt(1);


                Map<TopicPartition, KafkaTopicOffset> intersect = //merge?
                        beginOffset.keySet().stream()
                                .collect(toMap(
                                        key -> key,
                                        key -> {
                                            KafkaTopicOffset kafkaTopicOffset = new KafkaTopicOffset();
                                            kafkaTopicOffset.setStart(beginOffset.getOrDefault(key, 0L));
                                            kafkaTopicOffset.setEnd(endOffset.getOrDefault(key, 0L));
                                            return kafkaTopicOffset;
                                        }
                                ));
                handler.complete(intersect);


            } else {
                log.error("Can't combine topic begin,end offsets");
                handler.fail(ar.cause());
            }
        }));

    }

    private void initSystemTopicConsumer() {
        consumer.handler(record -> {
            byte[] key = record.key().getBytes();
            byte[] value;
            if (key != null) {
                Object o = GroupMetadataManager.readMessageKey(ByteBuffer.wrap(key));
                if (o instanceof OffsetKey) {
                    OffsetKey offsetKey = (OffsetKey) o;
                    value = record.value().getBytes();
                    if (value != null) {
                        ByteBuffer byteBuffer = ByteBuffer.wrap(value);
                        OffsetAndMetadata offsetAndMetadata =
                                GroupMetadataManager.readOffsetMessageValue(byteBuffer);
                        lastCommitedOffsets.computeIfPresent(offsetKey.key(),(k,v) -> {

                            if (offsetAndMetadata.commitTimestamp() > v.getLastCommitTimestamp())
                                v.setLastCommitTimestamp(offsetAndMetadata.commitTimestamp());

                            if (offsetAndMetadata.offset() > v.getOffset())
                                v.setOffset(offsetAndMetadata.offset());


                            return v;
                        });

                        lastCommitedOffsets.computeIfAbsent(offsetKey.key(),v -> {
                            KafkaTopicCommitedOffset kafkaTopicCommitedOffset = new KafkaTopicCommitedOffset();
                            kafkaTopicCommitedOffset.setLastCommitTimestamp(offsetAndMetadata.commitTimestamp());
                            kafkaTopicCommitedOffset.setOffset(offsetAndMetadata.offset());
                            return kafkaTopicCommitedOffset;
                        });
                    }
                }
            }
            //consumer.commit();
        });

        consumer.subscribe(SYSTEM_TOPIC, ar -> {
            if (ar.succeeded())
                log.info("Subscribed to system topic");
            else
                log.error("Subscription to system topic error",ar.cause());
        });
    }

    private void initTopicOffsetRefresh() {
        Integer monitorPoolingTimeoutMs = kafkaProperties.getAdmin().getMonitorPoolingTimeoutMs();
        if(monitorPoolingTimeoutMs == null)
            monitorPoolingTimeoutMs = 1000;

        this.vertx.setPeriodic(monitorPoolingTimeoutMs, handler ->
                getConsumerGroupNames().compose(this::getConsumerGroupTopicPartitions).onComplete(
                        ar -> {
                            if (ar.failed()) {
                                log.error("Can't get Group Consumer Info", ar.cause());
                            } else {

                                Map<String, Future<Map<TopicPartition, KafkaTopicOffset>>> offsets =
                                        ar.result().entrySet().stream()
                                                .filter(e -> !e.getValue().isEmpty()).collect(toMap(
                                                Map.Entry::getKey,
                                                e -> getBeginEndOffsets(e.getValue())
                                        ));

                                offsets.forEach((group, topics) -> topics.onComplete(offsetAr -> {
                                    if (offsetAr.succeeded()) {
                                        Map<TopicPartition, KafkaTopicOffset> tp = offsetAr.result();
                                        tp.forEach(
                                                (topic, topicOffset) -> lastOffsets.put(new GroupTopicPartition(group,
                                                                topic.getTopic(), topic.getPartition()),
                                                        topicOffset)
                                        );
                                    }
                                    else {
                                        log.error("Can't refresh kafka topic offsets",offsetAr.cause());
                                    }

                                }));
                            }

                        }
                ));
    }

    private Map<String, Map<TopicPartition, KafkaTopicCommitedOffset>> transformCommitedOffsets() {
        return lastCommitedOffsets.entrySet().stream().collect(
                groupingBy(e -> e.getKey().group(),
                        groupingBy(e -> new TopicPartition
                                        (e.getKey().topicPartition().topic(),
                                                e.getKey().topicPartition().partition())
                                , mapping(e -> {
                                    KafkaTopicCommitedOffset kafkaTopicCommitedOffset =
                                            new KafkaTopicCommitedOffset();
                                    kafkaTopicCommitedOffset.setOffset
                                            (e.getValue().getOffset());
                                    kafkaTopicCommitedOffset.setLastCommitTimestamp
                                            (e.getValue().getLastCommitTimestamp());
                                    return kafkaTopicCommitedOffset;
                                }, collectingAndThen(toList(), values -> values.get(0)))
                        )));

    }

    @Override
    public Map<KafkaGroupTopic, List<KafkaPartitionInfo>> getGroupConsumerInfo() {
        return lastOffsets.entrySet().stream().collect(groupingBy(e -> {
            KafkaGroupTopic kafkaGroupTopic = new KafkaGroupTopic();
            kafkaGroupTopic.setConsumerGroup(e.getKey().group());
            kafkaGroupTopic.setTopic(e.getKey().topicPartition().topic());
            return kafkaGroupTopic;
        }, mapping(e -> {
            KafkaPartitionInfo kafkaPartitionInfo = new KafkaPartitionInfo();
            kafkaPartitionInfo.setConsumerGroup(e.getKey().group());
            kafkaPartitionInfo.setTopic(e.getKey().topicPartition().topic());
            kafkaPartitionInfo.setPartition(e.getKey().topicPartition().partition());
            kafkaPartitionInfo.setStart(e.getValue().getStart());
            kafkaPartitionInfo.setEnd(e.getValue().getEnd());
            KafkaTopicCommitedOffset lastCommitedOffset = lastCommitedOffsets.getOrDefault(e.getKey(),
                    new KafkaTopicCommitedOffset(0L,0L));
            if (lastCommitedOffset != null) {
                kafkaPartitionInfo.setOffset(lastCommitedOffset.getOffset());
                kafkaPartitionInfo.setLastCommitTime(new Date(lastCommitedOffset.getLastCommitTimestamp()).toInstant()
                .atZone(ZoneId.systemDefault()).toLocalDateTime());
                kafkaPartitionInfo.setLag(Math.max(0L, e.getValue().getEnd() - lastCommitedOffset.getOffset()));
            }
            return kafkaPartitionInfo;
        }, toList())));
    }

    @Override
    public KafkaPartitionInfo getAggregateGroupConsumerInfo(String consumerGroup, String topic) {

        KafkaPartitionInfo result = new KafkaPartitionInfo();
        result.setConsumerGroup(consumerGroup);
        result.setTopic(topic);
        result.setPartition(0);
        result.setStart(0L);
        result.setEnd(0L);
        result.setOffset(0L);
        result.setLag(0L);
        result.setLastCommitTime(new Date(0L).toInstant()
                .atZone(ZoneId.systemDefault()).toLocalDateTime());

        return getGroupConsumerInfo().getOrDefault(new KafkaGroupTopic(consumerGroup,topic),new ArrayList<>())
                .stream().reduce(result,(l,r) -> {
                    l.setPartition(l.getPartition() + r.getPartition());
                    l.setStart(Math.min(l.getStart(),r.getStart()));
                    l.setEnd(l.getEnd() + r.getEnd());
                    l.setOffset(l.getOffset() + r.getOffset());
                    l.setLag(l.getLag() + r.getLag());
                    if(l.getLastCommitTime().compareTo(r.getLastCommitTime()) < 0)
                        l.setLastCommitTime(r.getLastCommitTime());
                    return l;
                });
    }
}
