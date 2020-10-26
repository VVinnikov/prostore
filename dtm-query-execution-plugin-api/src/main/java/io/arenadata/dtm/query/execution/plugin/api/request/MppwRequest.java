package io.arenadata.dtm.query.execution.plugin.api.request;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaParameter;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Request Mppw dto
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class MppwRequest extends DatamartRequest {

    /**
     * Sign of the start of mppw download
     */
    private Boolean isLoadStart;

    /**
     * Mppw params for loading to kafka
     */
    private MppwKafkaParameter kafkaParameter;

    @Builder
    public MppwRequest(QueryRequest queryRequest, Boolean isLoadStart, MppwKafkaParameter kafkaParameter) {
        super(queryRequest);
        this.isLoadStart = isLoadStart;
        this.kafkaParameter = kafkaParameter;
    }
}

