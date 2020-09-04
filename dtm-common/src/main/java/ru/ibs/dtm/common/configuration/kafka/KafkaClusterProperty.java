package ru.ibs.dtm.common.configuration.kafka;

import lombok.Data;

@Data
public class KafkaClusterProperty {
  String zookeeperHosts = "";
  Integer zookeeperPort = 2181;
  String rootPath = "";
}
