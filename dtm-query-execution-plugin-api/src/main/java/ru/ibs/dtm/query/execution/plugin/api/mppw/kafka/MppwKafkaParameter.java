package ru.ibs.dtm.query.execution.plugin.api.mppw.kafka;

import lombok.Builder;
import lombok.Data;
import ru.ibs.dtm.query.execution.plugin.api.edml.BaseExternalEntityMetadata;

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
