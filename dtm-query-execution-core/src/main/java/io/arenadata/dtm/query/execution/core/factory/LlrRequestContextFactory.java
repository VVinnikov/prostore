package io.arenadata.dtm.query.execution.core.factory;

import io.arenadata.dtm.common.cache.SourceQueryTemplateValue;
import io.arenadata.dtm.common.reader.QuerySourceRequest;
import io.arenadata.dtm.query.execution.core.dto.dml.DmlRequestContext;
import io.arenadata.dtm.query.execution.core.dto.dml.LlrRequestContext;
import io.vertx.core.Future;

public interface LlrRequestContextFactory {

    Future<LlrRequestContext> create(DmlRequestContext context);

    Future<LlrRequestContext> create(DmlRequestContext context, SourceQueryTemplateValue queryTemplateValue);
}
