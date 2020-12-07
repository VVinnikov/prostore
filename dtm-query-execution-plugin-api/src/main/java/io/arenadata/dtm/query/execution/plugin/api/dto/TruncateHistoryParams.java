package io.arenadata.dtm.query.execution.plugin.api.dto;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.reader.SourceType;
import lombok.Getter;

import java.util.Optional;

@Getter
public class TruncateHistoryParams extends PluginParams {
    private final Optional<Long> sysCn;
    private final String schema;
    private final String table;
    private final String env;
    private final Optional<String> conditions;

    public TruncateHistoryParams(SourceType type,
                                 RequestMetrics requestMetrics,
                                 Long sysCn,
                                 String schema,
                                 String table,
                                 String env,
                                 String conditions) {
        super(type, requestMetrics);
        this.sysCn = Optional.ofNullable(sysCn);
        this.schema = schema;
        this.table = table;
        this.env = env;
        this.conditions = Optional.ofNullable(conditions);
    }
}
