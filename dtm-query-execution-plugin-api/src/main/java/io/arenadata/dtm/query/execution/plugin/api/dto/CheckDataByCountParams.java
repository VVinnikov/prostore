package io.arenadata.dtm.query.execution.plugin.api.dto;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.SourceType;
import lombok.Getter;

@Getter
public class CheckDataByCountParams extends PluginParams{
    private final Entity entity;
    private final Long sysCn;

    public CheckDataByCountParams(SourceType sourceType,
                                  RequestMetrics requestMetrics,
                                  Entity entity,
                                  Long sysCn) {
        super(sourceType, requestMetrics);
        this.entity = entity;
        this.sysCn = sysCn;
    }
}
