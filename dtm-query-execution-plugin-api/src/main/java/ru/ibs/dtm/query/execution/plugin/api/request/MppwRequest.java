package ru.ibs.dtm.query.execution.plugin.api.request;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.plugin.api.mppw.parameter.KafkaParameter;

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
    private KafkaParameter kafkaParameter;

    @Builder
    public MppwRequest(QueryRequest queryRequest, Boolean isLoadStart, KafkaParameter kafkaParameter) {
        super(queryRequest);
        this.isLoadStart = isLoadStart;
        this.kafkaParameter = kafkaParameter;
    }
}

