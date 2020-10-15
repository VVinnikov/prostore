package ru.ibs.dtm.query.execution.plugin.adqm.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Mppr request for kafka connector
 *
 * @table table
 * @datamart datamart
 * @sql sql query
 * @zookeeperHost Zookeeper host (not used)
 * @zookeeperPort Zookeeper port (not used)
 * @kafkaTopic kafka topic
 * @chunkSize chunk size
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MpprKafkaConnectorRequest {
    String table;
    String datamart;
    String sql;
    String zookeeperHost;
    String zookeeperPort;
    String kafkaTopic;
    Integer chunkSize;
}
