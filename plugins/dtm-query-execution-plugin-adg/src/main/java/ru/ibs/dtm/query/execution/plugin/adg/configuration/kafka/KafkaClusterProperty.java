package ru.ibs.dtm.query.execution.plugin.adg.configuration.kafka;

import lombok.Data;

/**
 * Настройка кластера
 *
 * @zookeeperHosts хост Zookeeper
 * @rootPath путь до рута
 */
@Data
public class KafkaClusterProperty {
  String zookeeperHosts = "";
  String rootPath = "";
}
