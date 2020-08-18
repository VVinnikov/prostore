package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateSchema;
import org.apache.calcite.sql.ddl.SqlDropSchema;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.calcite.core.extension.ddl.SqlUseSchema;
import ru.ibs.dtm.query.calcite.core.extension.delta.SqlBeginDelta;
import ru.ibs.dtm.query.calcite.core.extension.delta.SqlCommitDelta;
import ru.ibs.dtm.query.calcite.core.extension.eddl.DropDatabase;
import ru.ibs.dtm.query.calcite.core.extension.eddl.SqlCreateDatabase;
import ru.ibs.dtm.query.calcite.core.service.DefinitionService;
import ru.ibs.dtm.query.execution.core.configuration.AppConfiguration;
import ru.ibs.dtm.query.execution.core.factory.RequestContextFactory;
import ru.ibs.dtm.query.execution.core.service.QueryAnalyzer;
import ru.ibs.dtm.query.execution.core.service.QueryDispatcher;
import ru.ibs.dtm.query.execution.core.utils.DatamartMnemonicExtractor;
import ru.ibs.dtm.query.execution.core.utils.DefaultDatamartSetter;
import ru.ibs.dtm.query.execution.core.utils.HintExtractor;
import ru.ibs.dtm.query.execution.plugin.api.RequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DatamartRequest;

@Slf4j
@Component
public class QueryAnalyzerImpl implements QueryAnalyzer {

    private final QueryDispatcher queryDispatcher;
    private final DefinitionService<SqlNode> definitionService;
    private final Vertx vertx;
    private final HintExtractor hintExtractor;
    private final RequestContextFactory<RequestContext<? extends DatamartRequest>, QueryRequest> requestContextFactory;
    private final AppConfiguration configuration;
    private final DatamartMnemonicExtractor datamartMnemonicExtractor;
    private final DefaultDatamartSetter defaultDatamartSetter;

    @Autowired
    public QueryAnalyzerImpl(QueryDispatcher queryDispatcher,
                             @Qualifier("coreCalciteDefinitionService") DefinitionService<SqlNode> definitionService,
                             RequestContextFactory<RequestContext<? extends DatamartRequest>, QueryRequest> requestContextFactory,
                             @Qualifier("coreVertx") Vertx vertx,
                             HintExtractor hintExtractor,
                             DatamartMnemonicExtractor datamartMnemonicExtractor,
                             AppConfiguration configuration,
                             DefaultDatamartSetter defaultDatamartSetter) {
        this.queryDispatcher = queryDispatcher;
        this.definitionService = definitionService;
        this.requestContextFactory = requestContextFactory;
        this.vertx = vertx;
        this.hintExtractor = hintExtractor;
        this.datamartMnemonicExtractor = datamartMnemonicExtractor;
        this.configuration = configuration;
        this.defaultDatamartSetter = defaultDatamartSetter;
    }

    @Override
    public void analyzeAndExecute(QueryRequest queryRequest, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        getParsedQuery(queryRequest, parseResult -> {
            if (parseResult.succeeded()) {
                try {
                    queryRequest.setSystemName(configuration.getSystemName());
                    ParsedQueryResponse parsedQueryResponse = parseResult.result();
                    SqlNode sqlNode = parsedQueryResponse.getSqlNode();
                    queryRequest.setSourceType(parsedQueryResponse.getSourceType());
                    if (existsDatamart(sqlNode)) {
                        if (Strings.isEmpty(queryRequest.getDatamartMnemonic())) {
                            val datamartMnemonic = datamartMnemonicExtractor.extract(sqlNode);
                            queryRequest.setDatamartMnemonic(datamartMnemonic);
                        } else {
                            sqlNode = defaultDatamartSetter.set(sqlNode, queryRequest.getDatamartMnemonic());
                        }
                    }
                    queryDispatcher.dispatch(
                            requestContextFactory.create(queryRequest, sqlNode), asyncResultHandler
                    );
                } catch (Exception ex) {
                    asyncResultHandler.handle(Future.failedFuture(ex));
                }
            } else {
                log.debug("Request parsing error", parseResult.cause());
                asyncResultHandler.handle(Future.failedFuture(parseResult.cause()));
            }
        });
    }

    private void getParsedQuery(QueryRequest queryRequest,
                                Handler<AsyncResult<ParsedQueryResponse>> asyncResultHandler) {
        vertx.executeBlocking(it ->
                {
                    try {
                        val hint = hintExtractor.extractHint(queryRequest);
                        val query = hint.getQueryRequest().getSql();
                        log.debug("Pre-parse request: {}", query);
                        val node = definitionService.processingQuery(query);
                        it.complete(new ParsedQueryResponse(node, query, hint.getSourceType()));
                    } catch (Exception e) {
                        log.error("Request parsing error", e);
                        it.fail(e);
                    }
                }
                , ar -> {
                    if (ar.succeeded()) {
                        asyncResultHandler.handle(Future.succeededFuture((ParsedQueryResponse) ar.result()));
                    } else {
                        asyncResultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }

    private boolean existsDatamart(SqlNode sqlNode) {
        return !(sqlNode instanceof SqlDropSchema)
                && !(sqlNode instanceof SqlCreateSchema)
                && !(sqlNode instanceof SqlCreateDatabase)
                && !(sqlNode instanceof DropDatabase)
                && !(sqlNode instanceof SqlBeginDelta)
                && !(sqlNode instanceof SqlCommitDelta)
                && !(sqlNode instanceof SqlUseSchema);
    }

    @Data
    private final static class ParsedQueryResponse {
        private final SqlNode sqlNode;
        private final String sqlWithoutHint;
        private final SourceType sourceType;
    }

}
