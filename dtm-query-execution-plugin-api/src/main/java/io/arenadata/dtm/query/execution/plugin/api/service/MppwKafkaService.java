package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequestContext;

public interface MppwKafkaService<T> extends DatamartExecutionService<MppwRequestContext, T> {

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.MPPW;
    }
}
