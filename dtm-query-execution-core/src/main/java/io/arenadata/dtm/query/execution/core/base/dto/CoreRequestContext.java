package io.arenadata.dtm.query.execution.core.base.dto;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.request.DatamartRequest;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.SqlNode;

@Getter
@Setter
public abstract class CoreRequestContext<R extends DatamartRequest, S extends SqlNode> {
    protected final RequestMetrics metrics;
    protected final String envName;
    protected final R request;
    protected S sqlNode;

    protected CoreRequestContext(RequestMetrics metrics, String envName, R request, S sqlNode) {
        this.metrics = metrics;
        this.envName = envName;
        this.request = request;
        this.sqlNode = sqlNode;
    }

    public abstract SqlProcessingType getProcessingType();
}
