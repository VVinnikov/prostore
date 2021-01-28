package io.arenadata.dtm.query.execution.plugin.api.mppr.kafka;

import io.arenadata.dtm.common.dto.KafkaBrokerInfo;
import io.arenadata.dtm.query.execution.plugin.api.edml.BaseExternalEntityMetadata;
import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class MpprKafkaParameter {
    private String datamart;
    private String dmlSubquery;
    private BaseExternalEntityMetadata downloadMetadata;
    private List<KafkaBrokerInfo> brokers;
    private String topic;
}
