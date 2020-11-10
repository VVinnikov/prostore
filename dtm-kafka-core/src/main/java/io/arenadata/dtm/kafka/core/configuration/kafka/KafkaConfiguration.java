package io.arenadata.dtm.kafka.core.configuration.kafka;


import io.arenadata.dtm.common.dto.KafkaBrokerInfo;
import io.arenadata.dtm.kafka.core.configuration.properties.KafkaProperties;
import io.arenadata.dtm.kafka.core.factory.KafkaProducerFactory;
import io.arenadata.dtm.kafka.core.factory.impl.VertxKafkaProducerFactory;
import io.arenadata.dtm.kafka.core.repository.ZookeeperKafkaProviderRepository;
import io.arenadata.dtm.kafka.core.service.kafka.KafkaConsumerMonitor;
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
@DependsOn({"coreKafkaProperties", "mapZkKafkaProviderRepository"})
public class KafkaConfiguration {

    private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    private static final String BROKERS_DELIMITER = ",";

    @Bean("coreKafkaProducerFactory")
    public KafkaProducerFactory<String, String> kafkaProviderFactory(@Qualifier("mapZkKafkaProviderRepository")
                                                                             ZookeeperKafkaProviderRepository zkConnProviderRepo,
                                                                     KafkaZookeeperProperties kafkaZkProperties,
                                                                     KafkaProperties kafkaProperties,
                                                                     @Qualifier("coreVertx") Vertx vertx) {
        Map<String, String> kafkaPropertyMap = new HashMap<>(kafkaProperties.getProducer().getProperty());
        String kafkaBrokers =
                zkConnProviderRepo.getOrCreate(kafkaZkProperties.getConnectionString()).getKafkaBrokers().stream()
                        .map(KafkaBrokerInfo::getAddress)
                        .collect(Collectors.joining(BROKERS_DELIMITER));
        kafkaPropertyMap.put(BOOTSTRAP_SERVERS, kafkaBrokers);
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
                                                               @Qualifier("mapZkKafkaProviderRepository")
                                                                       ZookeeperKafkaProviderRepository zkConnProviderRepo,
                                                               KafkaZookeeperProperties kafkaZkProperties) {
        Map<String, String> kafkaPropertyMap = new HashMap<>(kafkaProperties.getProducer().getProperty());
        String kafkaBrokers =
                zkConnProviderRepo.getOrCreate(kafkaZkProperties.getConnectionString()).getKafkaBrokers().stream()
                        .map(KafkaBrokerInfo::getAddress)
                        .collect(Collectors.joining(BROKERS_DELIMITER));
        kafkaPropertyMap.put(BOOTSTRAP_SERVERS, kafkaBrokers);
        return producerFactory.create(kafkaPropertyMap);
    }
}
