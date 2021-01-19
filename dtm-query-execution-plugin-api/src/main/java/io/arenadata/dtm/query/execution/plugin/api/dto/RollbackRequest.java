package io.arenadata.dtm.query.execution.plugin.api.dto;

import io.arenadata.dtm.common.model.ddl.Entity;
import lombok.Builder;
import lombok.Getter;

import java.util.UUID;

@Getter
public class RollbackRequest extends PluginRequest {

    private final String destinationTable;
    private final long sysCn;
    private final Entity entity;

    @Builder
    public RollbackRequest(UUID requestId,
                           String envName,
                           String datamartMnemonic,
                           String destinationTable,
                           long sysCn,
                           Entity entity) {
        super(requestId, envName, datamartMnemonic);
        this.destinationTable = destinationTable;
        this.sysCn = sysCn;
        this.entity = entity;
    }
}
