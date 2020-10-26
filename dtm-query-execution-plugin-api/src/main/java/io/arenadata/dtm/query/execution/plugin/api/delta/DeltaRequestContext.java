package io.arenadata.dtm.query.execution.plugin.api.delta;

import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.delta.query.DeltaQuery;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.SqlProcessingType;
import lombok.Data;
import lombok.ToString;

import static io.arenadata.dtm.query.execution.plugin.api.service.SqlProcessingType.DELTA;

@Data
@ToString
public class DeltaRequestContext extends RequestContext<DatamartRequest> {

    private DeltaQuery deltaQuery;

    public DeltaRequestContext(DatamartRequest request) {
        super(request);
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return DELTA;
    }
}
