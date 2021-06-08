package io.arenadata.dtm.query.execution.plugin.api.synchronize;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.plugin.api.dto.PluginRequest;
import lombok.Getter;
import lombok.ToString;

import java.util.UUID;

@Getter
@ToString
public class SynchronizeRequest extends PluginRequest {
    private final Entity entity;

    public SynchronizeRequest(UUID requestId, String envName, String datamartMnemonic, Entity entity) {
        super(requestId, envName, datamartMnemonic);
        this.entity = entity;
    }
}
