package io.arenadata.dtm.query.execution.core.check.dto;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.calcite.core.extension.check.CheckType;
import io.arenadata.dtm.query.calcite.core.extension.check.SqlCheckCall;
import io.arenadata.dtm.query.execution.core.base.dto.CoreRequestContext;
import lombok.Builder;

public class CheckContext extends CoreRequestContext<DatamartRequest, SqlCheckCall> {
    private final CheckType checkType;

    @Builder
    public CheckContext(RequestMetrics metrics,
                        String envName,
                        DatamartRequest request,
                        CheckType checkType,
                        SqlCheckCall sqlCheckCall) {
        super(metrics, envName, request, sqlCheckCall);
        this.checkType = checkType;
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return SqlProcessingType.CHECK;
    }

    public CheckType getCheckType() {
        return checkType;
    }
}
