package io.arenadata.dtm.query.execution.core.dml.factory;

import io.arenadata.dtm.common.cache.SourceQueryTemplateValue;
import io.arenadata.dtm.query.calcite.core.dto.delta.DeltaQueryPreprocessorResponse;
import io.arenadata.dtm.query.execution.core.dml.dto.DmlRequestContext;
import io.arenadata.dtm.query.execution.core.dml.dto.LlrRequestContext;
import io.vertx.core.Future;

public interface LlrRequestContextFactory {

    Future<LlrRequestContext> create(DmlRequestContext context);

    Future<LlrRequestContext> create(DeltaQueryPreprocessorResponse deltaResponse, DmlRequestContext context);

    Future<LlrRequestContext> create(DmlRequestContext context, SourceQueryTemplateValue queryTemplateValue);
}
