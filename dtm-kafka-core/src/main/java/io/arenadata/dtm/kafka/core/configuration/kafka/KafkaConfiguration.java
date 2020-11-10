package io.arenadata.dtm.kafka.core.configuration.kafka;


import io.arenadata.dtm.common.dto.KafkaBrokerInfo;
import io.arenadata.dtm.kafka.core.configuration.properties.KafkaProperties;
import io.arenadata.dtm.kafka.core.factory.KafkaProducerFactory;
import io.arenadata.dtm.kafka.core.factory.impl.VertxKafkaProducerFactory;
import io.arenadata.dtm.kafka.core.service.kafka.KafkaConsumerMonitor;
import io.arenadata.dtm.kafka.core.service.kafka.KafkaZookeeperConnectionProvider;
import io.arenadata.dtm.kafka.core.service.kafka.RestConsumerMonitorImpl;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;


@Configuration
@DependsOn({"coreKafkaProperties", "coreKafkaZkConnProviderMap"})
public class KafkaConfiguration {

    private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

    @Bean("coreKafkaProducerFactory")
    public KafkaProducerFactory<String, String> kafkaProviderFactory(@Qualifier("coreKafkaZkConnProviderMap")
                                                                             Map<String, KafkaZookeeperConnectionProvider> zkConnProviderMap,
                                                                     KafkaZookeeperProperties kafkaZkProperties,
                                                                     KafkaProperties kafkaProperties,
                                                                     @Qualifier("coreVertx") Vertx vertx) {
        Map<String, String> kafkaPropertyMap = new HashMap<>(kafkaProperties.getProducer().getProperty());
        final String kafkaBrokerList = getKafkaBrokersStr(zkConnProviderMap, kafkaZkProperties);
        kafkaPropertyMap.put("bootstrap.servers", kafkaBrokerList);
        return new VertxKafkaProducerFactory<>(vertx, kafkaPropertyMap);
    }

    @Bean("coreKafkaConsumerMonitor")
    public KafkaConsumerMonitor kafkaConsumerMonitor(@Qualifier("coreVertx") Vertx vertx,
                                                     KafkaProperties kafkaProperties) {
        return new RestConsumerMonitorImpl(vertx, kafkaProperties);
    }

    @Bean("jsonCoreKafkaProducer")
    public KafkaProducer<String, String> jsonCoreKafkaProducer(@Qualifier("coreKafkaProducerFactory") KafkaProducerFactory<String, String> producerFactory,
                                                               @Qualifier("coreKafkaProperties") KafkaProperties kafkaProperties,
                                                               @Qualifier("coreKafkaZkConnProviderMap")
                                                                       Map<String, KafkaZookeeperConnectionProvider> zkConnProviderMap,
                                                               KafkaZookeeperProperties kafkaZkProperties) {
        Map<String, String> kafkaPropertyMap = new HashMap<>(kafkaProperties.getProducer().getProperty());
        final String kafkaBrokerList = getKafkaBrokersStr(zkConnProviderMap, kafkaZkProperties);
        kafkaPropertyMap.put(BOOTSTRAP_SERVERS, kafkaBrokerList);
        return producerFactory.create(kafkaPropertyMap);
    }

    private String getKafkaBrokersStr(Map<String, KafkaZookeeperConnectionProvider> zkConnProviderMap, KafkaZookeeperProperties kafkaZkProperties) {
        return zkConnProviderMap.get(kafkaZkProperties.getConnectionString()).getKafkaBrokers().stream()
                .map(KafkaBrokerInfo::getAddress)
                .collect(Collectors.joining(","));
    }
}
