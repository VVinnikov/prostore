package io.arenadata.dtm.query.execution.plugin.api.request;

import io.arenadata.dtm.query.execution.plugin.api.dto.PluginRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaParameter;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.UUID;

/**
 * Request Mppw dto
 */
@Data
@ToString
@EqualsAndHashCode(callSuper = true)
public class MppwPluginRequest extends PluginRequest {

    /**
     * Sign of the start of mppw download
     */
    private Boolean isLoadStart;

    /**
     * Mppw params for loading to kafka
     */
    private MppwKafkaParameter kafkaParameter;

    @Builder
    public MppwPluginRequest(UUID requestId, String envName, String datamartMnemonic,
                             Boolean isLoadStart, MppwKafkaParameter kafkaParameter) {
        super(requestId, envName, datamartMnemonic);
        this.isLoadStart = isLoadStart;
        this.kafkaParameter = kafkaParameter;
    }
}

