package io.arenadata.dtm.query.execution.core.service.schema.impl;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.dto.schema.DatamartSchemaKey;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.core.service.schema.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.core.service.schema.LogicalSchemaService;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlNode;
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

    @Autowired
    public LogicalSchemaProviderImpl(LogicalSchemaService logicalSchemaService) {
        this.logicalSchemaService = logicalSchemaService;
    }

    @Override
    public Future<List<Datamart>> getSchemaFromQuery(SqlNode query, String datamart) {
        return logicalSchemaService.createSchemaFromQuery(query)
                .map(schemaMap -> {
                    log.trace("Received data schema on request: {}; {}", query, schemaMap);
                    return getDatamartsSchemas(datamart, schemaMap);
                });
    }

    @Override
    public Future<List<Datamart>> getSchemaFromDeltaInformations(List<DeltaInformation> deltaInformations, String datamart) {
        return logicalSchemaService.createSchemaFromDeltaInformations(deltaInformations)
                .map(schemaMap -> {
                    log.trace("Received data schema on delta information: {}; {}", deltaInformations, schemaMap);
                    return getDatamartsSchemas(datamart, schemaMap);
                });
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
    public Future<List<Datamart>> updateSchema(QueryRequest request) {
        //TODO implement
        return Future.failedFuture(new DtmException("Feature is not implemented"));
    }

}
