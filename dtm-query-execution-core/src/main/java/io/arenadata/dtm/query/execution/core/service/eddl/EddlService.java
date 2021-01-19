package io.arenadata.dtm.query.execution.core.service.eddl;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.core.dto.eddl.EddlRequestContext;
import io.arenadata.dtm.query.execution.core.service.DatamartExecutionService;

public interface EddlService<T> extends DatamartExecutionService<EddlRequestContext, T> {

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.EDDL;
    }

}
