package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import io.vertx.core.AsyncResult;


public interface LlrService<T> extends DatamartExecutionService<LlrRequestContext, AsyncResult<T>> {

	default SqlProcessingType getSqlProcessingType() {
		return SqlProcessingType.LLR;
	}

}
