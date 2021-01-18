package io.arenadata.dtm.common.request;

import io.arenadata.dtm.common.model.SqlProcessingType;
import lombok.Data;

@Data
public abstract class RequestContext<R extends DatamartRequest> {

    protected String envName;
    protected R request;

    public RequestContext(R request,
                          String envName) {
        this.request = request;
        this.envName = envName;
    }

    public R getRequest() {
        return request;
    }

    public abstract SqlProcessingType getProcessingType();
}
