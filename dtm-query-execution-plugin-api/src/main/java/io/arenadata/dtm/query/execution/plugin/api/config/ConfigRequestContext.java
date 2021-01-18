package io.arenadata.dtm.query.execution.plugin.api.config;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.SourceType;
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
public class ConfigRequestContext extends RequestContext<ConfigRequest, SqlConfigCall> {

    public ConfigRequestContext(RequestMetrics metrics,
                                ConfigRequest request,
                                SqlConfigCall sqlConfigCall,
                                String envName) {
        super(request, sqlConfigCall, envName, metrics);
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return CONFIG;
    }

}
