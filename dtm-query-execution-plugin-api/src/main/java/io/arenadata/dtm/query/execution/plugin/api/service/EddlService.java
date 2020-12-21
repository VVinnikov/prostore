package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.plugin.api.eddl.EddlRequestContext;

public interface EddlService<T> extends DatamartExecutionService<EddlRequestContext, T> {

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.EDDL;
    }

}
