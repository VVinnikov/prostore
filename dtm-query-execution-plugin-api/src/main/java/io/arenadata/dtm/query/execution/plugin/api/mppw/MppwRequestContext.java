package io.arenadata.dtm.query.execution.plugin.api.mppw;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.plugin.api.CoreRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.MppwRequest;
import lombok.ToString;

import static io.arenadata.dtm.common.model.SqlProcessingType.MPPW;

@ToString
public class MppwRequestContext extends CoreRequestContext<MppwRequest> {

    public MppwRequestContext(RequestMetrics metrics, MppwRequest request) {
        super(request, sqlNode, envName, metrics);
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return MPPW;
    }

    public MppwRequestContext copy() {
        return new MppwRequestContext(
                this.getMetrics(),
                new MppwRequest(
                        this.getRequest().getQueryRequest(),
                        this.getRequest().getIsLoadStart(),
                        this.getRequest().getKafkaParameter()));
    }
}
