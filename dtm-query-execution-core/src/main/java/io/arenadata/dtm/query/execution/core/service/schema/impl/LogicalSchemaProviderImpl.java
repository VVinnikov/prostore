package io.arenadata.dtm.query.execution.core.service.schema.impl;

import io.arenadata.dtm.common.dto.schema.DatamartSchemaKey;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.core.service.schema.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.core.service.schema.LogicalSchemaService;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class LogicalSchemaProviderImpl implements LogicalSchemaProvider {

    private final LogicalSchemaService logicalSchemaService;
    private final Map<DatamartSchemaKey, Entity> datamartSchemaMap;

    @Autowired
    public LogicalSchemaProviderImpl(LogicalSchemaService logicalSchemaService) {
        this.logicalSchemaService = logicalSchemaService;
        this.datamartSchemaMap = new HashMap<>();
    }

    @Override
    public void getSchema(QueryRequest request, Handler<AsyncResult<List<Datamart>>> resultHandler) {
        try {
            logicalSchemaService.createSchema(request, ar -> {
                if (ar.succeeded()) {
                    Map<DatamartSchemaKey, Entity> datamartTableMap = ar.result();
                    log.trace("Received data schema on request: {}; {}", request, datamartTableMap);
                    datamartSchemaMap.putAll(datamartTableMap);
                    resultHandler.handle(Future.succeededFuture(getDatamartsSchemas(request.getDatamartMnemonic(), datamartSchemaMap)));
                } else {
                    log.error("Error getting data schema for request: {}", request, ar.cause());
                    resultHandler.handle(Future.failedFuture(ar.cause()));
                }
            });
        } catch (Exception e) {
            log.error("Error in generating a logic diagram on request {}", request.getSql(), e);
            resultHandler.handle(Future.failedFuture(e));
        }
    }

    @NotNull
    private List<Datamart> getDatamartsSchemas(String defaultDatamart,
                                               Map<DatamartSchemaKey, Entity> datamartSchemaMap) {
        Map<String, Datamart> datamartMap = new HashMap<>();
        datamartSchemaMap.forEach((k, v) -> {
            final Datamart datamart = createDatamart(k.getSchema());
            if (datamart.getMnemonic().equals(defaultDatamart)) {
                datamart.setIsDefault(true);
            }
            datamartMap.putIfAbsent(k.getSchema(), datamart);
            datamartMap.get(k.getSchema()).getEntities().add(v);
        });
        return new ArrayList<>(datamartMap.values());
    }

    @NotNull
    private Datamart createDatamart(String schema) {
        Datamart datamart = new Datamart();
        datamart.setMnemonic(schema);
        datamart.setEntities(new ArrayList<>());
        return datamart;
    }

    @Override
    public void updateSchema(QueryRequest request, Handler<AsyncResult<List<Datamart>>> resultHandler) {
        //TODO implement
    }

}