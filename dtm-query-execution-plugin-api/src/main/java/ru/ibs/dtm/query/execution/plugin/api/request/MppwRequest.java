package ru.ibs.dtm.query.execution.plugin.api.request;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaParameter;

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

