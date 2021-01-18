package io.arenadata.dtm.query.execution.plugin.api;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import lombok.Data;
import org.apache.calcite.sql.SqlNode;

@Data
public abstract class RequestContext<R extends DatamartRequest, S extends SqlNode> {

    protected RequestMetrics metrics;
    protected String envName;
    protected S sqlNode;
    protected R request;

    public RequestContext(R request,
                          S sqlNode,
                          String envName) {
        this.request = request;
        this.sqlNode = sqlNode;
        this.envName = envName;
    }

    public RequestContext(R request,
                          S sqlNode,
                          String envName,
                          RequestMetrics metrics) {
        this.sqlNode = sqlNode;
        this.envName = envName;
        this.metrics = metrics;
        this.request = request;
    }

    public R getRequest() {
        return request;
    }

    public RequestMetrics getMetrics() {
        return metrics;
    }

    public void setMetrics(RequestMetrics metrics) {
        this.metrics = metrics;
    }

    public abstract SqlProcessingType getProcessingType();
}
