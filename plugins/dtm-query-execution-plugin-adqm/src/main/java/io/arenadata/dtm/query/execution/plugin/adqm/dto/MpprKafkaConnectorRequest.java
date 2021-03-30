package io.arenadata.dtm.query.execution.plugin.adqm.dto;

import io.arenadata.dtm.common.dto.KafkaBrokerInfo;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

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
    List<KafkaBrokerInfo> kafkaBrokers;
    String kafkaTopic;
    Integer chunkSize;
    String avroSchema;
    List<ColumnMetadata> metadata;
}
