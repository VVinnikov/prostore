package ru.ibs.dtm.query.execution.plugin.api.status;

import ru.ibs.dtm.query.execution.plugin.api.RequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.KafkaStatusRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType;

public class KafkaStatusRequestContext extends RequestContext<KafkaStatusRequest> {

    public KafkaStatusRequestContext(KafkaStatusRequest request) {
        super(request);
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return SqlProcessingType.STATUS;
    }
}
