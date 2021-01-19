package io.arenadata.dtm.query.execution.plugin.api.mppw;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.query.execution.plugin.api.dto.PluginRequest;
import io.arenadata.dtm.query.execution.plugin.api.edml.BaseExternalEntityMetadata;
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
public class MppwRequest extends PluginRequest {

    private final Entity sourceEntity;
    private final Long sysCn;
    private final String destinationTableName;
    private final BaseExternalEntityMetadata uploadMetadata;
    private final ExternalTableLocationType externalTableLocationType;
    /**
     * Sign of the start of mppw download
     */
    private Boolean isLoadStart;

    @Builder
    public MppwRequest(UUID requestId,
                       String envName,
                       String datamartMnemonic,
                       Boolean isLoadStart,
                       Entity sourceEntity,
                       Long sysCn,
                       String destinationTableName,
                       BaseExternalEntityMetadata uploadMetadata,
                       ExternalTableLocationType externalTableLocationType) {
        super(requestId, envName, datamartMnemonic);
        this.isLoadStart = isLoadStart;
        this.sourceEntity = sourceEntity;
        this.sysCn = sysCn;
        this.destinationTableName = destinationTableName;
        this.uploadMetadata = uploadMetadata;
        this.externalTableLocationType = externalTableLocationType;
    }
}

