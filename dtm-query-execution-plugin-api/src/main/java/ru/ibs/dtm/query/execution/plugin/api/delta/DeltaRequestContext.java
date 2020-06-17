package ru.ibs.dtm.query.execution.plugin.api.delta;

import lombok.Data;
import lombok.ToString;
import ru.ibs.dtm.query.execution.plugin.api.RequestContext;
import ru.ibs.dtm.query.execution.plugin.api.delta.query.DeltaQuery;
import ru.ibs.dtm.query.execution.plugin.api.request.DatamartRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType;

import static ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType.DELTA;

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
