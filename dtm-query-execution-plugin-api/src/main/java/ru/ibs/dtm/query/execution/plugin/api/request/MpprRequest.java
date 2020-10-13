package ru.ibs.dtm.query.execution.plugin.api.request;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;
import ru.ibs.dtm.query.execution.plugin.api.mppr.kafka.MpprKafkaParameter;

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

    @Builder
    public MpprRequest(QueryRequest queryRequest, MpprKafkaParameter kafkaParameter, List<Datamart> logicalSchema) {
        super(queryRequest);
        this.kafkaParameter = kafkaParameter;
        this.logicalSchema = logicalSchema;
    }
}
