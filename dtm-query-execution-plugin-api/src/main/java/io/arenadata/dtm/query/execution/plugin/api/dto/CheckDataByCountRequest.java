package io.arenadata.dtm.query.execution.plugin.api.dto;

import io.arenadata.dtm.common.model.ddl.Entity;
import lombok.Getter;

import java.util.UUID;

@Getter
public class CheckDataByCountRequest extends PluginRequest {
    private final Entity entity;
    private final Long sysCn;

    public CheckDataByCountRequest(Entity entity,
                                   Long sysCn,
                                   String envName,
                                   UUID requestId,
                                   String datamart) {
        super(requestId, envName, datamart);
        this.entity = entity;
        this.sysCn = sysCn;
    }
}
