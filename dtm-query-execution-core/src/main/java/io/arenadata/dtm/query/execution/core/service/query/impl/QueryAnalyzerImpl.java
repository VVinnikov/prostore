package io.arenadata.dtm.query.execution.core.service.query.impl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.*;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.calcite.core.extension.check.SqlCheckCall;
import io.arenadata.dtm.query.calcite.core.extension.config.function.SqlConfigStorageAdd;
import io.arenadata.dtm.query.calcite.core.extension.delta.SqlBeginDelta;
import io.arenadata.dtm.query.calcite.core.extension.delta.SqlCommitDelta;
import io.arenadata.dtm.query.calcite.core.extension.delta.SqlRollbackDelta;
import io.arenadata.dtm.query.calcite.core.extension.dml.SqlUseSchema;
import io.arenadata.dtm.query.calcite.core.extension.eddl.DropDatabase;
import io.arenadata.dtm.query.calcite.core.extension.eddl.SqlCreateDatabase;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.execution.core.dto.CoreRequestContext;
import io.arenadata.dtm.query.execution.core.factory.QueryRequestFactory;
import io.arenadata.dtm.query.execution.core.factory.RequestContextFactory;
import io.arenadata.dtm.query.execution.core.service.query.QueryAnalyzer;
import io.arenadata.dtm.query.execution.core.service.query.QueryDispatcher;
import io.arenadata.dtm.query.execution.core.service.query.QueryPreparedService;
import io.arenadata.dtm.query.execution.core.service.query.QuerySemicolonRemover;
import io.arenadata.dtm.query.execution.core.utils.DatamartMnemonicExtractor;
import io.arenadata.dtm.query.execution.core.utils.DefaultDatamartSetter;
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
    private final RequestContextFactory<CoreRequestContext<? extends DatamartRequest, ? extends SqlNode>, QueryRequest> requestContextFactory;
    private final DatamartMnemonicExtractor datamartMnemonicExtractor;
    private final DefaultDatamartSetter defaultDatamartSetter;
    private final QuerySemicolonRemover querySemicolonRemover;
    private final QueryRequestFactory queryRequestFactory;
    private final QueryPreparedService queryPreparedService;

    @Autowired
    public QueryAnalyzerImpl(QueryDispatcher queryDispatcher,
                             @Qualifier("coreCalciteDefinitionService") DefinitionService<SqlNode> definitionService,
                             RequestContextFactory<CoreRequestContext<? extends DatamartRequest, ? extends SqlNode>, QueryRequest> requestContextFactory,
                             @Qualifier("coreVertx") Vertx vertx,
                             DatamartMnemonicExtractor datamartMnemonicExtractor,
                             DefaultDatamartSetter defaultDatamartSetter,
                             QuerySemicolonRemover querySemicolonRemover,
                             QueryRequestFactory queryRequestFactory,
                             QueryPreparedService queryPreparedService) {
        this.queryDispatcher = queryDispatcher;
        this.definitionService = definitionService;
        this.requestContextFactory = requestContextFactory;
        this.vertx = vertx;
        this.datamartMnemonicExtractor = datamartMnemonicExtractor;
        this.defaultDatamartSetter = defaultDatamartSetter;
        this.queryRequestFactory = queryRequestFactory;
        this.queryPreparedService = queryPreparedService;
        this.querySemicolonRemover = querySemicolonRemover;
    }

    @Override
    public Future<QueryResult> analyzeAndExecute(InputQueryRequest execQueryRequest) {
        return getParsedQuery(execQueryRequest)
                .compose(this::dispatchQuery);
    }

    private Future<ParsedQueryResponse> getParsedQuery(InputQueryRequest inputQueryRequest) {
        return Future.future(promise -> vertx.executeBlocking(it -> {
            try {
                val request = querySemicolonRemover.remove(queryRequestFactory.create(inputQueryRequest));
                SqlNode node;
                if (request.getParameters() != null) {
                    log.debug("Try to get prepared query by request [{}]", request);
                    node = queryPreparedService.getPreparedQuery(request);
                } else {
                    node = definitionService.processingQuery(request.getSql());
                }
                it.complete(new ParsedQueryResponse(request, node));
            } catch (Exception e) {
                it.fail(new DtmException("Error parsing query", e));
            }
        }, promise));
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
            val requestContext = requestContextFactory.create(queryRequest, sqlNode);
            queryDispatcher.dispatch(requestContext)
                    .onComplete(promise);
        });
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
