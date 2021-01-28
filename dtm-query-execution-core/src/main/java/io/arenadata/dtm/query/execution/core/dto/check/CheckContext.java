package io.arenadata.dtm.query.execution.core.dto.check;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.calcite.core.extension.check.CheckType;
import io.arenadata.dtm.query.calcite.core.extension.check.SqlCheckCall;
import io.arenadata.dtm.query.execution.core.dto.CoreRequestContext;
import lombok.Builder;

public class CheckContext extends CoreRequestContext<DatamartRequest, SqlCheckCall> {
    private Entity entity;
    private CheckType checkType;
    private SqlCheckCall sqlCheckCall;

    @Builder
    public CheckContext(RequestMetrics metrics,
                        String envName,
                        DatamartRequest request,
                        CheckType checkType,
                        SqlCheckCall sqlCheckCall) {
        super(metrics, envName, request, sqlCheckCall);
        this.checkType = checkType;
        this.sqlCheckCall = sqlCheckCall;
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
