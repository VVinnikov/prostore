package io.arenadata.dtm.query.execution.plugin.api.request;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.api.mppr.kafka.MpprKafkaParameter;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * Request Mppr dto
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class MpprRequest extends DatamartRequest {

    /**
     * Mppr params for unloading from kafka
     */
    private MpprKafkaParameter kafkaParameter;
    /**
     * Logical schema
     */
    private List<Datamart> logicalSchema;
    /**
     * Column metadata list
     */
    private List<ColumnMetadata> metadata;
    /**
     * Destination entity
     */
    private Entity destinationEntity;

    @Builder
    public MpprRequest(MpprKafkaParameter kafkaParameter,
                       List<Datamart> logicalSchema,
                       List<ColumnMetadata> metadata,
                       Entity destinationEntity,
                       QueryRequest queryRequest) {
        super(queryRequest);
        this.kafkaParameter = kafkaParameter;
        this.logicalSchema = logicalSchema;
        this.metadata = metadata;
        this.destinationEntity = destinationEntity;
    }
}
