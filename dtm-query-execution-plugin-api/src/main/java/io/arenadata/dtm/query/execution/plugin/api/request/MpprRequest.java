package io.arenadata.dtm.query.execution.plugin.api.request;

import io.arenadata.dtm.common.reader.QueryRequest;
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

    @Builder
    public MpprRequest(QueryRequest queryRequest,
                       MpprKafkaParameter kafkaParameter,
                       List<Datamart> logicalSchema,
                       List<ColumnMetadata> metadata) {
        super(queryRequest);
        this.kafkaParameter = kafkaParameter;
        this.logicalSchema = logicalSchema;
        this.metadata = metadata;
    }
}
