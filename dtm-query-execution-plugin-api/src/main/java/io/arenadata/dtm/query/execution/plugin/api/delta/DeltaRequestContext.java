package io.arenadata.dtm.query.execution.plugin.api.delta;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import io.arenadata.dtm.common.model.SqlProcessingType;
import lombok.Data;
import lombok.ToString;

import static io.arenadata.dtm.common.model.SqlProcessingType.DELTA;

@Data
@ToString
public class DeltaRequestContext extends RequestContext<DatamartRequest> {

    public DeltaRequestContext(RequestMetrics metrics, DatamartRequest request) {
        super(metrics, request);
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return DELTA;
    }
}
