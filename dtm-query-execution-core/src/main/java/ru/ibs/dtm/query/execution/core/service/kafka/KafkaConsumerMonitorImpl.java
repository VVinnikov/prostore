package ru.ibs.dtm.query.execution.core.service.kafka;

import io.vertx.core.*;
import io.vertx.kafka.admin.ConsumerGroupListing;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.admin.MemberDescription;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import kafka.coordinator.group.GroupMetadataManager;
import kafka.coordinator.group.GroupTopicPartition;
import kafka.coordinator.group.OffsetKey;
import lombok.extern.slf4j.Slf4j;
import io.vertx.kafka.client.common.TopicPartition;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import kafka.common.OffsetAndMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.plugin.status.kafka.KafkaPartitionInfo;
import ru.ibs.dtm.common.plugin.status.kafka.KafkaTopicCommitedOffset;
import ru.ibs.dtm.common.plugin.status.kafka.KafkaTopicOffset;
import ru.ibs.dtm.query.execution.core.factory.KafkaConsumerFactory;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;


import static java.util.stream.Collectors.*;

@Component
@Slf4j
public class KafkaConsumerMonitorImpl implements KafkaConsumerMonitor {

    private static final String SYSTEM_TOPIC = "__consumer_offsets";
    private final KafkaAdminClient adminClient;
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final Vertx vertx;
    private final ConcurrentHashMap<GroupTopicPartition, KafkaTopicCommitedOffset> lastCommitedOffsets;
    private final ConcurrentHashMap<GroupTopicPartition, KafkaTopicOffset> lastOffsets;



    @Autowired
    public KafkaConsumerMonitorImpl(@Qualifier("coreKafkaAdminClient") KafkaAdminClient adminClient,
                                    @Qualifier("coreKafkaConsumerFactory") KafkaConsumerFactory<byte[], byte[]> consumerFactory,
                                    @Qualifier("coreVertx") Vertx vertx) /*core??*/ {
        this.adminClient = adminClient;

        // Set Properties
        HashMap<String, String> properties = new HashMap<>();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "monitoring_consumer_" + UUID.randomUUID().toString());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");


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
            byte[] key = record.key();
            byte[] value;
            if (key != null) {
                Object o = GroupMetadataManager.readMessageKey(ByteBuffer.wrap(key));
                if (o instanceof OffsetKey) {
                    OffsetKey offsetKey = (OffsetKey) o;
                    value = record.value();
                    if (value != null) {
                        ByteBuffer byteBuffer = ByteBuffer.wrap(value);
                        OffsetAndMetadata offsetAndMetadata =
                                GroupMetadataManager.readOffsetMessageValue(byteBuffer);

                        lastCommitedOffsets.computeIfPresent(offsetKey.key(),(k,v) -> {
                            Long last = v.getLastCommitTimestamp();
                            if (offsetAndMetadata.commitTimestamp() > last) {
                                KafkaTopicCommitedOffset kafkaTopicCommitedOffset = new KafkaTopicCommitedOffset();
                                kafkaTopicCommitedOffset.setLastCommitTimestamp(offsetAndMetadata.commitTimestamp());
                                kafkaTopicCommitedOffset.setOffset(offsetAndMetadata.offset());
                                return kafkaTopicCommitedOffset;
                            }
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
                log.error("Subscription to system topic error");
        });
    }

    public void initTopicOffsetRefresh() {
        this.vertx.setPeriodic(1000, handler ->
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
                                        log.error("Can't refresh kafka topic offsets");
                                    }

                                }));
                            }

                        }
                ));
    }

    private Map<String, Map<TopicPartition, KafkaTopicCommitedOffset>> transformCommitedOffsets() {
        return lastCommitedOffsets.entrySet().stream().collect(
                groupingBy(e -> e.getKey().group().toString(),
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
    public List<KafkaPartitionInfo> getGroupConsumerInfo() {
         return new ArrayList<>(lastOffsets.entrySet().stream().collect(toMap(
                 Map.Entry::getKey,
                 e -> {
                     KafkaPartitionInfo kafkaPartitionInfo = new KafkaPartitionInfo();
                     kafkaPartitionInfo.setConsumerGroup(e.getKey().group().toString());
                     kafkaPartitionInfo.setTopic(e.getKey().topicPartition().topic());
                     kafkaPartitionInfo.setPartition(e.getKey().topicPartition().partition());
                     kafkaPartitionInfo.setStart(e.getValue().getStart());
                     kafkaPartitionInfo.setEnd(e.getValue().getEnd());
                     KafkaTopicCommitedOffset lastCommitedOffset = lastCommitedOffsets.get(e.getKey());
                     if (lastCommitedOffset != null) {
                         kafkaPartitionInfo.setOffset(lastCommitedOffset.getOffset());
                         kafkaPartitionInfo.setLastCommitTime(new Date(lastCommitedOffset.getLastCommitTimestamp()));
                         kafkaPartitionInfo.setLag(Math.max(0L, e.getValue().getEnd() - lastCommitedOffset.getOffset()));
                     }
                     return kafkaPartitionInfo;
                 }
         )).values());

    }

    /*
    @Override
    public Future<List<Future<List<KafkaPartitionInfo>>>> getGroupConsumerInfo() {
        return Future.future(handler ->
                getConsumerGroupNames().compose(this::getConsumerGroupTopicPartitions).onComplete(
                        ar -> {
                            if (ar.failed()) {
                                log.error("Can't get Group Consumer Info", ar.cause());
                                handler.complete(new ArrayList<>());
                            } else {

                                Map<String, Future<Map<TopicPartition, KafkaTopicOffset>>> offsets =
                                        ar.result().entrySet().stream()
                                                .filter(e-> !e.getValue().isEmpty()).collect(toMap(
                                                Map.Entry::getKey,
                                                e -> getBeginEndOffsets(e.getValue())
                                        ));

                                Map<String, Map<TopicPartition, KafkaTopicCommitedOffset>> commitedOffsets =
                                        transformCommitedOffsets();


                                /// merge?
                                /// hack?
                                List<Future<List<KafkaPartitionInfo>>> result =
                                        new ArrayList<>(offsets.entrySet().stream().collect(toMap(
                                        Map.Entry::getKey,
                                        e -> e.getValue().map(fut -> fut.entrySet().stream().map(ie -> {
                                                    KafkaPartitionInfo info = new KafkaPartitionInfo();
                                                    info.setConsumerGroup(e.getKey());
                                                    info.setTopic(ie.getKey().getTopic());
                                                    info.setPartition(ie.getKey().getPartition());
                                                    info.setStart(ie.getValue().getStart());
                                                    info.setEnd(ie.getValue().getEnd());
                                                    ///
                                                    if (commitedOffsets.get(e.getKey()) != null)
                                                        if (commitedOffsets.get(e.getKey()).get(ie.getKey()) != null) {
                                                            info.setOffset(commitedOffsets.get(e.getKey())
                                                                    .get(ie.getKey()).getOffset());
                                                            long timestamp = commitedOffsets.get(e.getKey())
                                                                    .get(ie.getKey()).getLastCommitTimestamp();
                                                            info.setLastCommitTime
                                                                    (LocalDateTime.ofInstant
                                                                            (Instant.ofEpochSecond(timestamp),
                                                                                    TimeZone.getDefault().toZoneId()));
                                                        }
                                                    return info;
                                                }
                                        ).collect(toList())))).values());

                                //.entrySet().stream().flatMap(Map.Entry::getValue).collect(toList());
                                handler.complete(result);
                            }

                        }
                ));

    } */


}
