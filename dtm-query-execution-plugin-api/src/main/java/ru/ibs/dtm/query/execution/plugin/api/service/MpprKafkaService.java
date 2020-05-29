package ru.ibs.dtm.query.execution.plugin.api.service;

import io.vertx.core.AsyncResult;
import ru.ibs.dtm.query.execution.plugin.api.mppr.MpprRequestContext;


public interface MpprKafkaService<T> extends DatamartExecutionService<MpprRequestContext, AsyncResult<T>> {

	default SqlProcessingType getSqlProcessingType() {
		return SqlProcessingType.MPPR;
	}

}
