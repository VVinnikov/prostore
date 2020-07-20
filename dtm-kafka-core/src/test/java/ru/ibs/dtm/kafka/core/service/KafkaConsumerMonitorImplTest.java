package ru.ibs.dtm.kafka.core.service;

import com.salesforce.kafka.test.KafkaTestUtils;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.kafka.admin.KafkaAdminClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import ru.ibs.dtm.common.plugin.status.kafka.KafkaGroupTopic;
import ru.ibs.dtm.common.plugin.status.kafka.KafkaPartitionInfo;
import ru.ibs.dtm.kafka.core.configuration.properties.KafkaProperties;
import ru.ibs.dtm.kafka.core.factory.KafkaConsumerFactory;
import ru.ibs.dtm.kafka.core.factory.impl.VertxKafkaConsumerFactory;
import ru.ibs.dtm.kafka.core.service.kafka.KafkaConsumerMonitorImpl;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.IntStream.range;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Slf4j
class KafkaConsumerMonitorImplTest {

    private static final int WAIT_TIMEOUT = 10000;

    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource()
            .withBrokers(4);

    private final Vertx vertx = Vertx.vertx();
    private final HashMap<String, String> coreKafkaConfig = new HashMap<String, String>() {{
        put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,sharedKafkaTestResource.getKafkaConnectString());
        put(ConsumerConfig.GROUP_ID_CONFIG,"core-monitor");
        put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

    }};

    private final KafkaProperties kafkaProperties = new KafkaProperties();

    private final KafkaAdminClient adminClient = KafkaAdminClient.create(vertx, coreKafkaConfig );
    KafkaConsumerFactory<byte[], byte[]> kafkaConsumerFactory =
            new VertxKafkaConsumerFactory(vertx,coreKafkaConfig);

    KafkaConsumerMonitorImpl monitor = new KafkaConsumerMonitorImpl(adminClient,kafkaConsumerFactory,vertx,kafkaProperties);

    void createTestTopic(List<String> topics,int partitionsCount, short replicasCount) {
        KafkaTestUtils kafkaTestUtils = sharedKafkaTestResource.getKafkaTestUtils();
        topics.forEach(topic ->
                kafkaTestUtils.createTopic(topic,partitionsCount,replicasCount));
    }



    void produceTestMessages(Map<String, Integer> topicsWithMessageCount) {
        KafkaTestUtils kafkaTestUtils = sharedKafkaTestResource.getKafkaTestUtils();

        KafkaProducer<String, String> producer =
                kafkaTestUtils.getKafkaProducer(StringSerializer.class, StringSerializer.class);


        topicsWithMessageCount.keySet().forEach(topic ->
                {

                    range(0,topicsWithMessageCount.get(topic)).forEach(iter -> {
                        final String testKey = RandomStringUtils.random(10,true,true);
                        final String testMessage = RandomStringUtils.random(50,true,true);
                        final ProducerRecord<String,String> producerRecord =
                                new ProducerRecord<>(topic,testMessage);
                        producer.send(producerRecord);
                        producer.flush();
                    });
                });
        wait(WAIT_TIMEOUT);
    }

    void consumeTestMessages(List<String> topics, String consumerGroupName, boolean commit, boolean close) {
        KafkaTestUtils kafkaTestUtils = sharedKafkaTestResource.getKafkaTestUtils();

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,consumerGroupName);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"5000");
        KafkaConsumer<String, String> consumer =
                kafkaTestUtils.getKafkaConsumer(StringDeserializer.class, StringDeserializer.class
                ,props);

        consumer.subscribe(topics);
        ConsumerRecords<String, String> records;
        do {
            records = consumer.poll(Duration.ofSeconds(5L));
        } while (!records.isEmpty());


        if(commit)
            consumer.commitSync(Duration.ofSeconds(5L));

        wait(WAIT_TIMEOUT / 2);

        if(close)
            consumer.close();

        wait(WAIT_TIMEOUT / 2);
    }

    public static void wait(int ms)
    {
        try
        {
            Thread.sleep(ms);
        }
        catch(InterruptedException ex)
        {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void getGroupConsumerInfo()  {
        List<String> testTopics = Arrays.asList("TEST1","TEST2","TEST3");
        createTestTopic(testTopics,4,(short) 1);

        Map<String, Integer> toSend = testTopics.stream().collect(toMap(
                e -> e, e -> 100
        ));

        produceTestMessages(toSend);

        assertTrue(monitor.getGroupConsumerInfo()
                .containsKey(new KafkaGroupTopic("core-monitor","__consumer_offsets")));


        consumeTestMessages(testTopics,"test_consumer",false, false);

        assertTrue(monitor.getGroupConsumerInfo()
                .containsKey(new KafkaGroupTopic("test_consumer","TEST1")));
        assertTrue(monitor.getGroupConsumerInfo()
                .containsKey(new KafkaGroupTopic("test_consumer","TEST2")));
        assertTrue(monitor.getGroupConsumerInfo()
                .containsKey(new KafkaGroupTopic("test_consumer","TEST3")));

        assertEquals(monitor.getGroupConsumerInfo()
                .get(new KafkaGroupTopic("test_consumer","TEST1")).size(),4);
        assertEquals(monitor.getGroupConsumerInfo()
                .get(new KafkaGroupTopic("test_consumer","TEST2")).size(),4);
        assertEquals(monitor.getGroupConsumerInfo()
                .get(new KafkaGroupTopic("test_consumer","TEST3")).size(),4);

        assertEquals(monitor.getGroupConsumerInfo()
                .get(new KafkaGroupTopic("test_consumer","TEST1"))
                .stream().map(KafkaPartitionInfo::getOffset)
                .reduce(0L, Long::sum),
                0);

        assertEquals(monitor.getGroupConsumerInfo()
                        .get(new KafkaGroupTopic("test_consumer","TEST2"))
                        .stream().map(KafkaPartitionInfo::getOffset)
                        .reduce(0L, Long::sum),
                0);
        assertEquals(monitor.getGroupConsumerInfo()
                        .get(new KafkaGroupTopic("test_consumer","TEST3"))
                        .stream().map(KafkaPartitionInfo::getOffset)
                        .reduce(0L, Long::sum),
                0);


        produceTestMessages(toSend);
        consumeTestMessages(testTopics,"test_consumer_3",true,true);
        assertTrue(monitor.getGroupConsumerInfo()
                .containsKey(new KafkaGroupTopic("test_consumer_3","TEST1")));
        assertTrue(monitor.getGroupConsumerInfo()
                .containsKey(new KafkaGroupTopic("test_consumer_3","TEST2")));
        assertTrue(monitor.getGroupConsumerInfo()
                .containsKey(new KafkaGroupTopic("test_consumer_3","TEST3")));


        assertEquals(monitor.getGroupConsumerInfo()
                        .get(new KafkaGroupTopic("test_consumer_3","TEST1"))
                        .stream().map(KafkaPartitionInfo::getEnd)
                        .reduce(0L, Long::sum),
                200);

        assertEquals(monitor.getGroupConsumerInfo()
                        .get(new KafkaGroupTopic("test_consumer_3","TEST2"))
                        .stream().map(KafkaPartitionInfo::getEnd)
                        .reduce(0L, Long::sum),
                200);
        assertEquals(monitor.getGroupConsumerInfo()
                        .get(new KafkaGroupTopic("test_consumer_3","TEST3"))
                        .stream().map(KafkaPartitionInfo::getEnd)
                        .reduce(0L, Long::sum),
                200);
    }

    @Test
    void getAggregateGroupConsumerInfo() {
        List<String> testTopics = Collections.singletonList("TEST_INFO");
        createTestTopic(testTopics,4,(short) 1);

        Map<String, Integer> toSend = testTopics.stream().collect(toMap(
                e -> e, e -> 100
        ));

        consumeTestMessages(testTopics,"test_consumer_4",false,false);

        assertEquals(monitor.getAggregateGroupConsumerInfo("test_consumer_4","TEST_INFO").getPartition(),
                0 + 1 + 2 + 3);

        produceTestMessages(toSend);

        assertEquals(monitor.getAggregateGroupConsumerInfo("test_consumer_4","TEST_INFO").getEnd(),
                100);

        consumeTestMessages(testTopics,"test_consumer_5",true,true);


        assertEquals(monitor.getAggregateGroupConsumerInfo("test_consumer_5","TEST_INFO").getOffset(),
                100);

        assertEquals(monitor.getAggregateGroupConsumerInfo("test_consumer_5","TEST_INFO")
                .getLastCommitTime().getDayOfYear(),
                LocalDate.now().getDayOfYear());
        LocalDateTime prev = monitor.getAggregateGroupConsumerInfo("test_consumer_5","TEST_INFO")
                .getLastCommitTime();

        produceTestMessages(toSend);

        consumeTestMessages(testTopics,"test_consumer_5",true,true);
        assertTrue(monitor.getAggregateGroupConsumerInfo("test_consumer_5","TEST_INFO")
                        .getLastCommitTime().compareTo(prev) > 0);

        assertEquals(monitor.getAggregateGroupConsumerInfo("test_consumer_5","TEST_INFO").getOffset(),
                200);

        produceTestMessages(toSend);
        consumeTestMessages(testTopics,"test_consumer_5",true,true);

        assertEquals(monitor.getAggregateGroupConsumerInfo("test_consumer_5","TEST_INFO").getOffset(),
                300);
        assertTrue(monitor.getAggregateGroupConsumerInfo("test_consumer_5","TEST_INFO")
                .getLastCommitTime().compareTo(prev) > 0);


    }



}