package ru.ibs.dtm.query.execution.plugin.api.mppr.kafka;

import lombok.Builder;
import lombok.Data;
import ru.ibs.dtm.query.execution.plugin.api.edml.BaseExternalEntityMetadata;

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
