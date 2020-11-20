package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import io.vertx.core.AsyncResult;


public interface MpprKafkaService<T> extends DatamartExecutionService<MpprRequestContext, AsyncResult<T>> {

	default SqlProcessingType getSqlProcessingType() {
		return SqlProcessingType.MPPR;
	}

}
