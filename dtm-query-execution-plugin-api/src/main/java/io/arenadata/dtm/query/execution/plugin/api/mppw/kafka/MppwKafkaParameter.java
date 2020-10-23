package io.arenadata.dtm.query.execution.plugin.api.mppw.kafka;

import io.arenadata.dtm.query.execution.plugin.api.edml.BaseExternalEntityMetadata;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class MppwKafkaParameter {
    private String datamart;
    private long sysCn;
    private String targetTableName;
    private BaseExternalEntityMetadata uploadMetadata;
    private String zookeeperHost;
    private int zookeeperPort;
    private String topic;
}
