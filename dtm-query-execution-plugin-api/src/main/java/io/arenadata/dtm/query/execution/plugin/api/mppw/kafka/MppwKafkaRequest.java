package io.arenadata.dtm.query.execution.plugin.api.mppw.kafka;

import io.arenadata.dtm.common.dto.KafkaBrokerInfo;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.query.execution.plugin.api.edml.BaseExternalEntityMetadata;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequest;
import lombok.Builder;
import lombok.Getter;

import java.util.List;
import java.util.UUID;

@Getter
public class MppwKafkaRequest extends MppwRequest {
    private final List<KafkaBrokerInfo> brokers;
    private final String topic;

    @Builder(toBuilder = true)
    public MppwKafkaRequest(UUID requestId,
                            String envName,
                            String datamartMnemonic,
                            Boolean isLoadStart,
                            Entity sourceEntity,
                            Long sysCn,
                            String destinationTableName,
                            BaseExternalEntityMetadata uploadMetadata,
                            List<KafkaBrokerInfo> brokers,
                            String topic) {
        super(requestId,
                envName,
                datamartMnemonic,
                isLoadStart,
                sourceEntity,
                sysCn,
                destinationTableName,
                uploadMetadata,
                ExternalTableLocationType.KAFKA);
        this.brokers = brokers;
        this.topic = topic;
    }
}
