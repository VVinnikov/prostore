package io.arenadata.dtm.query.execution.core.service.delta.impl;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.calcite.core.extension.delta.SqlBeginDelta;
import io.arenadata.dtm.query.calcite.core.extension.delta.SqlCommitDelta;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.execution.core.service.delta.DeltaQueryParamExtractor;
import io.arenadata.dtm.query.execution.plugin.api.delta.query.BeginDeltaQuery;
import io.arenadata.dtm.query.execution.plugin.api.delta.query.CommitDeltaQuery;
import io.arenadata.dtm.query.execution.plugin.api.delta.query.DeltaQuery;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Component
@Slf4j
public class DeltaQueryParamExtractorImpl implements DeltaQueryParamExtractor {

    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private final DefinitionService<SqlNode> definitionService;
    private final Vertx coreVertx;

    @Autowired
    public DeltaQueryParamExtractorImpl(
            @Qualifier("coreCalciteDefinitionService") DefinitionService<SqlNode> definitionService,
            Vertx coreVertx
    ) {
        this.definitionService = definitionService;
        this.coreVertx = coreVertx;
    }

    @Override
    public void extract(QueryRequest request, Handler<AsyncResult<DeltaQuery>> asyncResultHandler) {
        coreVertx.executeBlocking(it -> {
            try {
                SqlNode node = definitionService.processingQuery(request.getSql());
                it.complete(node);
            } catch (Exception e) {
                log.error("Request parsing error", e);
                it.fail(e);
            }
        }, ar -> {
            if (ar.succeeded()) {
                SqlNode sqlNode = (SqlNode) ar.result();
                extract(sqlNode, asyncResultHandler);
            } else {
                asyncResultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private void extract(SqlNode sqlNode, Handler<AsyncResult<DeltaQuery>> asyncResultHandler) {
        if (sqlNode instanceof SqlBeginDelta) {
            createBeginDeltaQuery((SqlBeginDelta) sqlNode, asyncResultHandler);
        } else if (sqlNode instanceof SqlCommitDelta) {
            createCommitDeltaQuery((SqlCommitDelta) sqlNode, asyncResultHandler);
        } else {
            asyncResultHandler.handle(Future.failedFuture("Query [" + sqlNode + "] is not a DELTA operator."));
        }
    }

    private void createBeginDeltaQuery(SqlBeginDelta sqlNode, Handler<AsyncResult<DeltaQuery>> asyncResultHandler) {
        BeginDeltaQuery deltaQuery = new BeginDeltaQuery();
        deltaQuery.setDeltaNum(sqlNode.getDeltaNumOperator().getNum());
        log.debug("Retrieved beginDeltaQuery parameters: {}", deltaQuery);
        asyncResultHandler.handle(Future.succeededFuture(deltaQuery));
    }

    private void createCommitDeltaQuery(SqlCommitDelta sqlNode, Handler<AsyncResult<DeltaQuery>> asyncResultHandler) {
        try {
            CommitDeltaQuery deltaQuery = new CommitDeltaQuery();
            deltaQuery.setDeltaDateTime(getDeltaDateTime(sqlNode));
            log.debug("Extracted parameters commitDeltaQuery: {}", deltaQuery);
            asyncResultHandler.handle(Future.succeededFuture(deltaQuery));
        } catch (RuntimeException e) {
            log.error("Parameter conversion error 'dateTime'", e);
            asyncResultHandler.handle(Future.failedFuture(e.getMessage()));
        }
    }

    private LocalDateTime getDeltaDateTime(SqlCommitDelta sqlNode) {
        val deltaDateTime = sqlNode.getDeltaDateTimeOperator().getDeltaDateTime();
        if (deltaDateTime != null) {
            return LocalDateTime.parse(deltaDateTime, DATE_TIME_FORMATTER);
        } else {
            return null;
        }
    }
}
