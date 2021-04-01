package io.arenadata.dtm.query.execution.core.config.dto;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.calcite.core.extension.config.SqlConfigCall;
import io.arenadata.dtm.query.execution.core.base.dto.CoreRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.ConfigRequest;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static io.arenadata.dtm.common.model.SqlProcessingType.CONFIG;

@Data
@ToString
@EqualsAndHashCode(callSuper = true)
public class ConfigRequestContext extends CoreRequestContext<ConfigRequest, SqlConfigCall> {

    @Builder
    public ConfigRequestContext(RequestMetrics metrics,
                                ConfigRequest request,
                                SqlConfigCall sqlConfigCall,
                                String envName) {
        super(metrics, envName, request, sqlConfigCall);
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return CONFIG;
    }

}
