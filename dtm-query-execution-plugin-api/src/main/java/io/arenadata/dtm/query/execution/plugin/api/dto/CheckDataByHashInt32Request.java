package io.arenadata.dtm.query.execution.plugin.api.dto;

import io.arenadata.dtm.common.model.ddl.Entity;
import lombok.Getter;

import java.util.Set;

@Getter
public class CheckDataByHashInt32Request extends PluginRequest {
    private final Entity entity;
    private final Long sysCn;
    private final Set<String> columns;
    private final String env;

    public CheckDataByHashInt32Request(Entity entity,
                                       Long sysCn,
                                       Set<String> columns,
                                       String env) {
        this.entity = entity;
        this.sysCn = sysCn;
        this.columns = columns;
        this.env = env;
    }
}
