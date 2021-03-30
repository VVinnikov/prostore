package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.plugin.api.dto.PluginRequest;
import io.vertx.core.Future;

public interface DatamartExecutionService<RQ extends PluginRequest, RS> {

    Future<RS> execute(RQ request);

    SqlProcessingType getSqlProcessingType();
}
