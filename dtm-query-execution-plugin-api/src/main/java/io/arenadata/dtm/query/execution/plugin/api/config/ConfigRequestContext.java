package io.arenadata.dtm.query.execution.plugin.api.config;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.calcite.core.extension.config.SqlConfigCall;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.ConfigRequest;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static io.arenadata.dtm.common.model.SqlProcessingType.CONFIG;

@Data
@ToString
@EqualsAndHashCode(callSuper = true)
public class ConfigRequestContext extends RequestContext<ConfigRequest> {

    private final SqlConfigCall sqlConfigCall;

    public ConfigRequestContext(RequestMetrics metrics,
                                ConfigRequest request,
                                SqlConfigCall sqlConfigCall) {
        super(request, sqlNode, envName, sourceType, metrics);
        this.sqlConfigCall = sqlConfigCall;
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return CONFIG;
    }

    @SuppressWarnings("unchecked")
    public <T extends SqlConfigCall> T getSqlConfigCall() {
        return (T) sqlConfigCall;
    }

}
