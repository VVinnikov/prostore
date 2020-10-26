package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.query.execution.plugin.api.eddl.EddlRequestContext;
import io.vertx.core.AsyncResult;


public interface EddlService<T> extends DatamartExecutionService<EddlRequestContext, AsyncResult<T>> {

	default SqlProcessingType getSqlProcessingType() {
		return SqlProcessingType.EDDL;
	}

}
