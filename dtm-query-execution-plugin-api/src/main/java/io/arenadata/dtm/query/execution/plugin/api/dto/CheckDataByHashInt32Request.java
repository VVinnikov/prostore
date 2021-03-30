package io.arenadata.dtm.query.execution.plugin.api.dto;

import io.arenadata.dtm.common.model.ddl.Entity;
import lombok.Builder;
import lombok.Getter;

import java.util.Set;
import java.util.UUID;

@Getter
public class CheckDataByHashInt32Request extends PluginRequest {
    private final Entity entity;
    private final Long sysCn;
    private final Set<String> columns;

    @Builder
    public CheckDataByHashInt32Request(Entity entity,
                                       Long sysCn,
                                       Set<String> columns,
                                       String envName,
                                       UUID requestId,
                                       String datamart) {
        super(requestId, envName, datamart);
        this.entity = entity;
        this.sysCn = sysCn;
        this.columns = columns;
    }
}
