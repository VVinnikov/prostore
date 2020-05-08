package ru.ibs.dtm.query.execution.plugin.adb.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Входящий запрос
 *
 * @table таблица
 * @datamart название витрины
 * @sql запрос на выборку
 * @zookeeperHost хост Zookeeper (сейчас не используется)
 * @zookeeperPort порт Zookeeper (сейчас не используется)
 * @kafkaTopic топик для выгрузки
 * @chunkSize размер чанка
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MpprKafkaConnectorRequest {
  String table;
  String datamart;
  String sql;
  String zookeeperHost;
  String zookeeperPort;
  String kafkaTopic;
  Integer chunkSize;
}
