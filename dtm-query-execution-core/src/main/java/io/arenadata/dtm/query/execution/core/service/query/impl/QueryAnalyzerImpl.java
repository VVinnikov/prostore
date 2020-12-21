package io.arenadata.dtm.query.execution.core.service.query.impl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.InputQueryRequest;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.QuerySourceRequest;
import io.arenadata.dtm.query.calcite.core.extension.check.SqlCheckCall;
import io.arenadata.dtm.query.calcite.core.extension.config.function.SqlConfigStorageAdd;
import io.arenadata.dtm.query.calcite.core.extension.ddl.SqlUseSchema;
import io.arenadata.dtm.query.calcite.core.extension.delta.SqlBeginDelta;
import io.arenadata.dtm.query.calcite.core.extension.delta.SqlCommitDelta;
import io.arenadata.dtm.query.calcite.core.extension.delta.SqlRollbackDelta;
import io.arenadata.dtm.query.calcite.core.extension.eddl.DropDatabase;
import io.arenadata.dtm.query.calcite.core.extension.eddl.SqlCreateDatabase;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.execution.core.factory.QueryRequestFactory;
import io.arenadata.dtm.query.execution.core.factory.RequestContextFactory;
import io.arenadata.dtm.query.execution.core.service.query.QuerySemicolonRemover;
import io.arenadata.dtm.query.execution.core.service.query.QueryAnalyzer;
import io.arenadata.dtm.query.execution.core.service.query.QueryDispatcher;
import io.arenadata.dtm.query.execution.core.utils.DatamartMnemonicExtractor;
import io.arenadata.dtm.query.execution.core.utils.DefaultDatamartSetter;
import io.arenadata.dtm.query.execution.core.utils.HintExtractor;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import io.vertx.core.Future;
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

@Slf4j
@Component
public class QueryAnalyzerImpl implements QueryAnalyzer {

    private final QueryDispatcher queryDispatcher;
    private final DefinitionService<SqlNode> definitionService;
    private final Vertx vertx;
    private final HintExtractor hintExtractor;
    private final RequestContextFactory<RequestContext<? extends DatamartRequest>, QueryRequest> requestContextFactory;
    private final DatamartMnemonicExtractor datamartMnemonicExtractor;
    private final DefaultDatamartSetter defaultDatamartSetter;
    private final QuerySemicolonRemover querySemicolonRemover;
    private final QueryRequestFactory queryRequestFactory;

    @Autowired
    public QueryAnalyzerImpl(QueryDispatcher queryDispatcher,
                             @Qualifier("coreCalciteDefinitionService") DefinitionService<SqlNode> definitionService,
                             RequestContextFactory<RequestContext<? extends DatamartRequest>, QueryRequest> requestContextFactory,
                             @Qualifier("coreVertx") Vertx vertx,
                             HintExtractor hintExtractor,
                             DatamartMnemonicExtractor datamartMnemonicExtractor,
                             DefaultDatamartSetter defaultDatamartSetter,
                             QuerySemicolonRemover querySemicolonRemover,
                             QueryRequestFactory queryRequestFactory) {
        this.queryDispatcher = queryDispatcher;
        this.definitionService = definitionService;
        this.requestContextFactory = requestContextFactory;
        this.vertx = vertx;
        this.hintExtractor = hintExtractor;
        this.datamartMnemonicExtractor = datamartMnemonicExtractor;
        this.defaultDatamartSetter = defaultDatamartSetter;
        this.querySemicolonRemover = querySemicolonRemover;
        this.queryRequestFactory = queryRequestFactory;
    }

    @Override
    public Future<QueryResult> analyzeAndExecute(InputQueryRequest execQueryRequest) {
        return getParsedQuery(execQueryRequest)
                .compose(this::dispatchQuery);
    }

    private Future<QueryResult> dispatchQuery(ParsedQueryResponse parsedQueryResponse) {
        return Future.future(promise -> {
            SqlNode sqlNode = parsedQueryResponse.getSqlNode();
            QueryRequest queryRequest = parsedQueryResponse.getQueryRequest();
            if (existsDatamart(sqlNode)) {
                if (Strings.isEmpty(queryRequest.getDatamartMnemonic())) {
                    val datamartMnemonic = datamartMnemonicExtractor.extract(sqlNode);
                    queryRequest.setDatamartMnemonic(datamartMnemonic);
                } else {
                    sqlNode = defaultDatamartSetter.set(sqlNode, queryRequest.getDatamartMnemonic());
                }
            }
            queryDispatcher.dispatch(requestContextFactory.create(queryRequest, sqlNode))
                    .onComplete(promise);
        });
    }

    private Future<ParsedQueryResponse> getParsedQuery(InputQueryRequest inputQueryRequest) {
        return Future.future(promise -> vertx.executeBlocking(it -> {
            try {
                val queryRequest = queryRequestFactory.create(inputQueryRequest);
                val queryRequestWithoutHint = getQueryRequestWithoutHint(queryRequest);
                queryRequest.setSourceType(queryRequestWithoutHint.getSourceType());
                log.debug("Pre-parse request: {}", queryRequestWithoutHint.getQueryRequest().getSql());
                val node = definitionService.processingQuery(queryRequestWithoutHint.getQueryRequest().getSql());
                it.complete(new ParsedQueryResponse(queryRequest, node));
            } catch (Exception e) {
                it.fail(new DtmException("Error parsing query", e));
            }
        }, promise));
    }

    private QuerySourceRequest getQueryRequestWithoutHint(QueryRequest queryRequest) {
        val withoutSemicolon = querySemicolonRemover.remove(queryRequest);
        return hintExtractor.extractHint(withoutSemicolon);
    }

    private boolean existsDatamart(SqlNode sqlNode) {
        return !(sqlNode instanceof SqlDropSchema)
                && !(sqlNode instanceof SqlCreateSchema)
                && !(sqlNode instanceof SqlCreateDatabase)
                && !(sqlNode instanceof DropDatabase)
                && !(sqlNode instanceof SqlBeginDelta)
                && !(sqlNode instanceof SqlCommitDelta)
                && !(sqlNode instanceof SqlRollbackDelta)
                && !(sqlNode instanceof SqlUseSchema)
                && !(sqlNode instanceof SqlConfigStorageAdd)
                && !(sqlNode instanceof SqlCheckCall);
    }

    @Data
    private final static class ParsedQueryResponse {
        private final QueryRequest queryRequest;
        private final SqlNode sqlNode;
    }

}