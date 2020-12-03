package io.arenadata.dtm.query.execution.plugin.api.dto;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.reader.SourceType;

public class TruncateHistoryParams extends PluginParams {
    private final Long sysCn;
    private final String schema;
    private final String table;
    private final String env;

    public TruncateHistoryParams(SourceType type,
                                 RequestMetrics requestMetrics,
                                 Long sysCn,
                                 String schema,
                                 String table,
                                 String env) {
        super(type, requestMetrics);
        this.sysCn = sysCn;
        this.schema = schema;
        this.table = table;
        this.env = env;
    }

    public Long getSysCn() {
        return sysCn;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public String getEnv() {
        return env;
    }
}
