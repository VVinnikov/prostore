package io.arenadata.dtm.query.execution.plugin.api.mppr.kafka;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.dto.KafkaBrokerInfo;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.api.edml.BaseExternalEntityMetadata;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequest;
import lombok.Builder;
import lombok.Getter;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.UUID;

@Getter
public class MpprKafkaRequest extends MpprRequest {
    private final SqlNode dmlSubQuery;
    private final BaseExternalEntityMetadata downloadMetadata;
    private final List<KafkaBrokerInfo> brokers;
    private final String topic;
    private final String sql;

    @Builder
    public MpprKafkaRequest(UUID requestId,
                            String envName,
                            String datamartMnemonic,
                            SqlNode sqlNode,
                            List<Datamart> logicalSchema,
                            List<ColumnMetadata> metadata,
                            Entity destinationEntity,
                            List<DeltaInformation> deltaInformations,
                            SqlNode dmlSubQuery,
                            BaseExternalEntityMetadata downloadMetadata,
                            List<KafkaBrokerInfo> brokers,
                            String topic, String sql) {
        super(requestId,
                envName,
                datamartMnemonic,
                sqlNode,
                logicalSchema,
                metadata,
                destinationEntity,
                deltaInformations,
                ExternalTableLocationType.KAFKA);
        this.dmlSubQuery = dmlSubQuery;
        this.downloadMetadata = downloadMetadata;
        this.brokers = brokers;
        this.topic = topic;
        this.sql = sql;
    }
}
