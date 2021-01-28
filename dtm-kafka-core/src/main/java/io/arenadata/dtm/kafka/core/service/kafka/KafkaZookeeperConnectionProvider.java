package io.arenadata.dtm.kafka.core.service.kafka;

import io.arenadata.dtm.common.dto.KafkaBrokerInfo;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;

public interface KafkaZookeeperConnectionProvider {

    ZooKeeper getOrConnect();

    List<KafkaBrokerInfo> getKafkaBrokers();

    void close();
}
