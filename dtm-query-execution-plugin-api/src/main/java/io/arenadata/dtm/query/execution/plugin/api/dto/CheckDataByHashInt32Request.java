package io.arenadata.dtm.query.execution.plugin.api.dto;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.SourceType;
import lombok.Getter;

import java.util.Set;

@Getter
public class CheckDataByHashInt32Request extends PluginRequest {
    private final Entity entity;
    private final Long sysCn;
    private final Set<String> columns;
    private final String env;

    public CheckDataByHashInt32Request(SourceType sourceType,
                                       RequestMetrics requestMetrics,
                                       Entity entity,
                                       Long sysCn,
                                       Set<String> columns,
                                       String env) {
        super(sourceType, requestMetrics);
        this.entity = entity;
        this.sysCn = sysCn;
        this.columns = columns;
        this.env = env;
    }
}
