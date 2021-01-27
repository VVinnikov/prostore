package io.arenadata.dtm.common.configuration.kafka;

import lombok.Data;

@Data
public class KafkaClusterProperty {
  String zookeeperHost = "";
  Integer zookeeperPort = 2181;
}
