package io.arenadata.dtm.query.execution.core.service.ddl;

import io.arenadata.dtm.common.ddl.TruncateType;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.calcite.core.extension.ddl.truncate.SqlBaseTruncate;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;

public class TruncateContext extends RequestContext<DatamartRequest> {
    private final TruncateType truncateType;
    private final SqlBaseTruncate sqlBaseTruncate;

    public TruncateContext(RequestMetrics metrics,
                           DatamartRequest request,
                           TruncateType truncateType,
                           SqlBaseTruncate sqlBaseTruncate) {
        super(metrics, request);
        this.truncateType = truncateType;
        this.sqlBaseTruncate = sqlBaseTruncate;
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return SqlProcessingType.TRUNCATE;
    }

    public TruncateType getTruncateType() {
        return truncateType;
    }

    public SqlBaseTruncate getSqlBaseTruncate() {
        return sqlBaseTruncate;
    }
}
