package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequestContext;


public interface MpprKafkaService<T> extends DatamartExecutionService<MpprRequestContext, T> {

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.MPPR;
    }

}
