package io.arenadata.dtm.query.execution.plugin.api.dto;

import io.arenadata.dtm.common.model.ddl.Entity;
import lombok.Getter;

@Getter
public class CheckDataByCountRequest extends PluginRequest {
    private final Entity entity;
    private final Long sysCn;
    private final String env;

    public CheckDataByCountRequest(Entity entity,
                                   Long sysCn,
                                   String env) {
        this.entity = entity;
        this.sysCn = sysCn;
        this.env = env;
    }
}
