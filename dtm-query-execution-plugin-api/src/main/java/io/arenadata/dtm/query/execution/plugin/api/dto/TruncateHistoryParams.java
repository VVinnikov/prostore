package io.arenadata.dtm.query.execution.plugin.api.dto;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.SourceType;
import lombok.Getter;
import org.apache.calcite.sql.SqlNode;

import java.util.Optional;

@Getter
public class TruncateHistoryParams extends PluginParams {
    private final Optional<Long> sysCn;
    private final Entity entity;
    private final String env;
    private final Optional<SqlNode> conditions;

    public TruncateHistoryParams(SourceType type,
                                 RequestMetrics requestMetrics,
                                 Long sysCn,
                                 Entity entity,
                                 String env,
                                 SqlNode conditions) {
        super(type, requestMetrics);
        this.sysCn = Optional.ofNullable(sysCn);
        this.entity = entity;
        this.env = env;
        this.conditions = Optional.ofNullable(conditions);
    }
}
