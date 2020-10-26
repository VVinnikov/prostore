package io.arenadata.dtm.query.execution.plugin.api.mppr.kafka;

import io.arenadata.dtm.query.execution.plugin.api.edml.BaseExternalEntityMetadata;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class MpprKafkaParameter {
    private String datamart;
    private String dmlSubquery;
    private BaseExternalEntityMetadata downloadMetadata;
    private String zookeeperHost;
    private int zookeeperPort;
    private String topic;
}
