package io.arenadata.dtm.query.execution.plugin.api.check;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.calcite.core.extension.check.CheckType;
import io.arenadata.dtm.query.calcite.core.extension.check.SqlCheckCall;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;

public class CheckContext extends RequestContext<DatamartRequest> {
    private Entity entity;
    private CheckType checkType;
    private SqlCheckCall sqlCheckCall;

    public CheckContext(RequestMetrics metrics,
                        DatamartRequest request,
                        CheckType checkType,
                        SqlCheckCall sqlCheckCall) {
        super(metrics, request);
        this.checkType = checkType;
        this.sqlCheckCall = sqlCheckCall;
    }

    public CheckContext(RequestMetrics metrics, DatamartRequest request, Entity entity) {
        super(metrics, request);
        this.entity = entity;
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return SqlProcessingType.CHECK;
    }

    public Entity getEntity() {
        return entity;
    }

    public CheckType getCheckType() {
        return checkType;
    }

    public SqlCheckCall getSqlCheckCall() {
        return sqlCheckCall;
    }
}
