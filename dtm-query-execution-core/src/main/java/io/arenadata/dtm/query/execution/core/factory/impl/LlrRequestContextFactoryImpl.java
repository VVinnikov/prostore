package io.arenadata.dtm.query.execution.core.factory.impl;

import io.arenadata.dtm.common.cache.SourceQueryTemplateValue;
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.reader.QuerySourceRequest;
import io.arenadata.dtm.query.calcite.core.dto.delta.DeltaQueryPreprocessorResponse;
import io.arenadata.dtm.query.calcite.core.service.DeltaQueryPreprocessor;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.core.calcite.CoreCalciteDMLQueryParserService;
import io.arenadata.dtm.query.execution.core.dto.dml.DmlRequestContext;
import io.arenadata.dtm.query.execution.core.dto.dml.LlrRequestContext;
import io.arenadata.dtm.query.execution.core.factory.LlrRequestContextFactory;
import io.arenadata.dtm.query.execution.core.service.dml.ColumnMetadataService;
import io.arenadata.dtm.query.execution.core.service.schema.LogicalSchemaProvider;
import io.vertx.core.Future;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class LlrRequestContextFactoryImpl implements LlrRequestContextFactory {

    private final LogicalSchemaProvider logicalSchemaProvider;
    private final ColumnMetadataService columnMetadataService;
    private final DeltaQueryPreprocessor deltaQueryPreprocessor;
    private final QueryParserService parserService;

    @Autowired
    public LlrRequestContextFactoryImpl(LogicalSchemaProvider logicalSchemaProvider,
                                        ColumnMetadataService columnMetadataService,
                                        DeltaQueryPreprocessor deltaQueryPreprocessor,
                                        CoreCalciteDMLQueryParserService parserService) {
        this.logicalSchemaProvider = logicalSchemaProvider;
        this.columnMetadataService = columnMetadataService;
        this.deltaQueryPreprocessor = deltaQueryPreprocessor;
        this.parserService = parserService;
    }

    @Override
    public Future<LlrRequestContext> create(DmlRequestContext context) {
        LlrRequestContext llrContext = createLlrRequestContext(context);
        return initDeltaInformations(llrContext)
                .compose(v -> initLlrContext(llrContext));
    }

    @Override
    public Future<LlrRequestContext> create(DeltaQueryPreprocessorResponse deltaResponse, DmlRequestContext context) {
        LlrRequestContext llrContext = createLlrRequestContext(context);
        llrContext.setDeltaInformations(deltaResponse.getDeltaInformations());
        llrContext.getDmlRequestContext().setSqlNode(deltaResponse.getSqlNode());
        return initLlrContext(llrContext);
    }

    @Override
    public Future<LlrRequestContext> create(DmlRequestContext context, SourceQueryTemplateValue queryTemplateValue) {
        LlrRequestContext llrContext = createLlrRequestContext(context);
        llrContext.getSourceRequest().setMetadata(queryTemplateValue.getMetadata());
        llrContext.getSourceRequest().setLogicalSchema(queryTemplateValue.getLogicalSchema());
        llrContext.getSourceRequest().getQueryRequest().setSql(queryTemplateValue.getSql());
        llrContext.setDeltaInformations(queryTemplateValue.getDeltaInformations());
        llrContext.setQueryTemplateValue(queryTemplateValue);
        return Future.succeededFuture(llrContext);
    }

    private LlrRequestContext createLlrRequestContext(DmlRequestContext context) {
        val sourceRequest = new QuerySourceRequest(context.getRequest().getQueryRequest(),
                context.getSqlNode(),
                context.getSourceType());
        return LlrRequestContext.builder()
                .sourceRequest(sourceRequest)
                .dmlRequestContext(context)
                .build();
    }

    private Future<LlrRequestContext> initDeltaInformations(LlrRequestContext llrContext) {
        return deltaQueryPreprocessor.process(llrContext.getDmlRequestContext().getSqlNode())
                .map(preprocessorResponse -> {
                    llrContext.setDeltaInformations(preprocessorResponse.getDeltaInformations());
                    llrContext.getDmlRequestContext().setSqlNode(preprocessorResponse.getSqlNode());
                    return llrContext;
                });
    }

    private Future<LlrRequestContext> initLlrContext(LlrRequestContext llrContext) {
        return logicalSchemaProvider.getSchemaFromQuery(
                llrContext.getDmlRequestContext().getSqlNode(),
                llrContext.getDmlRequestContext().getRequest().getQueryRequest().getDatamartMnemonic())
                .map(schema -> {
                    llrContext.getSourceRequest().setLogicalSchema(schema);
                    return llrContext;
                })
                .compose(v -> parserService.parse(new QueryParserRequest(llrContext.getSourceRequest().getQueryTemplate().getTemplateNode(),
                        llrContext.getSourceRequest().getLogicalSchema())))
                .map(response -> {
                    llrContext.setRelNode(response.getRelNode());
                    return response;
                })
                .compose(response -> columnMetadataService.getColumnMetadata(response.getRelNode()))
                .map(metadata -> {
                    llrContext.getSourceRequest().setMetadata(metadata);
                    return llrContext;
                });
    }
}
