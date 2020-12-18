package io.arenadata.dtm.query.execution.core.service.delta.impl;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaQuery;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.query.execution.core.factory.DeltaQueryFactory;
import io.arenadata.dtm.query.execution.core.service.delta.DeltaQueryParamExtractor;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class DeltaQueryParamExtractorImpl implements DeltaQueryParamExtractor {

    private final DefinitionService<SqlNode> definitionService;
    private final DeltaQueryFactory deltaQueryFactory;
    private final Vertx coreVertx;

    @Autowired
    public DeltaQueryParamExtractorImpl(
            @Qualifier("coreCalciteDefinitionService") DefinitionService<SqlNode> definitionService,
            DeltaQueryFactory deltaQueryFactory, Vertx coreVertx) {
        this.definitionService = definitionService;
        this.deltaQueryFactory = deltaQueryFactory;
        this.coreVertx = coreVertx;
    }

    @Override
    public Future<DeltaQuery> extract(QueryRequest request) {
        return Future.future(promise -> {
            coreVertx.executeBlocking(it -> {
                try {
                    SqlNode node = definitionService.processingQuery(request.getSql());
                    it.complete(node);
                } catch (Exception e) {
                    it.fail(new DtmException("Error parsing sql query", e));
                }
            }, ar -> {
                if (ar.succeeded()) {
                    SqlNode sqlNode = (SqlNode) ar.result();
                    try {
                        final DeltaQuery deltaQuery = deltaQueryFactory.create(sqlNode);
                        log.debug("Delta query created successfully: {}", deltaQuery);
                        promise.complete(deltaQuery);
                    } catch (Exception e) {
                        promise.fail(new DtmException("Error creating delta query from sql node", e));
                    }
                } else {
                    promise.fail(ar.cause());
                }
            });
        });

    }
}
