package io.arenadata.dtm.query.execution.core.eddl.service;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.core.eddl.dto.EddlRequestContext;
import io.arenadata.dtm.query.execution.core.base.service.DatamartExecutionService;

public interface EddlService<T> extends DatamartExecutionService<EddlRequestContext, T> {

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.EDDL;
    }

}
