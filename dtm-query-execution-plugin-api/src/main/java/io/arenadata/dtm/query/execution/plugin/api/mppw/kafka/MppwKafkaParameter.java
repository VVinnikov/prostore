package io.arenadata.dtm.query.execution.plugin.api.mppw.kafka;

import io.arenadata.dtm.common.dto.KafkaBrokerInfo;
import io.arenadata.dtm.query.execution.plugin.api.edml.BaseExternalEntityMetadata;
import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class MppwKafkaParameter {
    private String datamart;
    private long sysCn;
    private String targetTableName;
    private BaseExternalEntityMetadata uploadMetadata;
    private List<KafkaBrokerInfo> brokers;
    private String topic;
}
