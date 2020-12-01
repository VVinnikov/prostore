package io.arenadata.dtm.query.execution.plugin.api.dto;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.SourceType;
import lombok.Getter;

import java.util.List;

@Getter
public class CheckDataByHashInt32Params extends PluginParams{
    private final Entity entity;
    private final Long sysCn;
    private final List<String> columns;

    public CheckDataByHashInt32Params(SourceType sourceType,
                                      RequestMetrics requestMetrics,
                                      Entity entity,
                                      Long sysCn,
                                      List<String> columns) {
        super(sourceType, requestMetrics);
        this.entity = entity;
        this.sysCn = sysCn;
        this.columns = columns;
    }
}
