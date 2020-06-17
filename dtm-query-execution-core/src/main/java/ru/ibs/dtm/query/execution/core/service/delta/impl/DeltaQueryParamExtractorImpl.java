package ru.ibs.dtm.query.execution.core.service.delta.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.core.calcite.delta.SqlBeginDelta;
import ru.ibs.dtm.query.execution.core.calcite.delta.SqlCommitDelta;
import ru.ibs.dtm.query.execution.core.service.DefinitionService;
import ru.ibs.dtm.query.execution.core.service.delta.DeltaQueryParamExtractor;
import ru.ibs.dtm.query.execution.core.service.eddl.impl.EddlQueryParamExtractorImpl;
import ru.ibs.dtm.query.execution.plugin.api.delta.query.BeginDeltaQuery;
import ru.ibs.dtm.query.execution.plugin.api.delta.query.CommitDeltaQuery;
import ru.ibs.dtm.query.execution.plugin.api.delta.query.DeltaQuery;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Component
@Slf4j
public class DeltaQueryParamExtractorImpl implements DeltaQueryParamExtractor {

    private static final Logger LOGGER = LoggerFactory.getLogger(EddlQueryParamExtractorImpl.class);
    private final DefinitionService<SqlNode> definitionService;
    private final Vertx coreVertx;

    @Autowired
    public DeltaQueryParamExtractorImpl(DefinitionService<SqlNode> definitionService, Vertx coreVertx) {
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
                LOGGER.error("Ошибка парсинга запроса", e);
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
            asyncResultHandler.handle(Future.failedFuture("Запрос [" + sqlNode + "] не является DELTA оператором."));
        }
    }

    private void createBeginDeltaQuery(SqlBeginDelta sqlNode, Handler<AsyncResult<DeltaQuery>> asyncResultHandler) {
        BeginDeltaQuery deltaQuery = new BeginDeltaQuery();
        deltaQuery.setDeltaNum(sqlNode.getDeltaNumOperator().getNum());
        asyncResultHandler.handle(Future.succeededFuture(deltaQuery));
    }

    private void createCommitDeltaQuery(SqlCommitDelta sqlNode, Handler<AsyncResult<DeltaQuery>> asyncResultHandler) {
        try {
            CommitDeltaQuery deltaQuery = new CommitDeltaQuery();
            deltaQuery.setDeltaDateTime(getDeltaDateTime(sqlNode));
            asyncResultHandler.handle(Future.succeededFuture(deltaQuery));
        } catch (RuntimeException e) {
            LOGGER.error("Ошибка преобразования параметра 'dateTime'", e);
            asyncResultHandler.handle(Future.failedFuture(e.getMessage()));
        }
    }

    private LocalDateTime getDeltaDateTime(SqlCommitDelta sqlNode) {
        if (sqlNode.getDeltaDateTimeOperator().getDeltaDateTime() != null) {
            return LocalDateTime.parse(sqlNode.getDeltaDateTimeOperator().getDeltaDateTime(),
                    DateTimeFormatter.ISO_DATE_TIME);
        } else {
            return null;
        }
    }
}
