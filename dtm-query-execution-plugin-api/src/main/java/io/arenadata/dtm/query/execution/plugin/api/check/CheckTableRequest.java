package io.arenadata.dtm.query.execution.plugin.api.check;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.plugin.api.dto.PluginRequest;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.UUID;

@EqualsAndHashCode(callSuper = true)
@Data
public class CheckTableRequest extends PluginRequest {
    private final Entity entity;

    public CheckTableRequest(UUID requestId,
                             String envName,
                             String datamartMnemonic,
                             Entity entity) {
        super(requestId, envName, datamartMnemonic);
        this.entity = entity;
    }
}
