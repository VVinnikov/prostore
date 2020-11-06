package io.arenadata.dtm.query.execution.core.service.dml;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QuerySourceRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.service.InformationSchemaService;
import io.arenadata.dtm.query.execution.core.service.dml.impl.InformationSchemaDefinitionServiceImpl;
import io.arenadata.dtm.query.execution.core.service.impl.InformationSchemaServiceImpl;
import io.vertx.core.Promise;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class InformationSchemaDefinitionServiceImplTest {

    private final InformationSchemaService informationSchemaService = mock(InformationSchemaServiceImpl.class);
    private InformationSchemaDefinitionService informationSchemaDefinitionService;

    @BeforeEach
    void setUp() {
        informationSchemaDefinitionService = new InformationSchemaDefinitionServiceImpl(informationSchemaService);
    }

    @Test
    void getInfoSchemaSuccess() {
        Promise promise = Promise.promise();
        QuerySourceRequest sourceRequest = new QuerySourceRequest();
        final QueryRequest queryRequest = new QueryRequest(UUID.randomUUID(),
                "dtm",
                "select * from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = 'DEMO'");
        queryRequest.setDeltaInformations(Collections.singletonList(
                DeltaInformation.builder()
                        .schemaName("dtm")
                        .build()
        ));
        sourceRequest.setQueryRequest(queryRequest);
        Map<String, Entity> entityMap = new HashMap<>();
        entityMap.put("logic_schema_entities", Entity.builder()
                .name("logic_schema_entities")
                .schema("dtm")
                .build());

        when(informationSchemaService.getEntities()).thenReturn(entityMap);

        informationSchemaDefinitionService.tryGetInformationSchemaRequest(sourceRequest)
                .onComplete(handler -> {
                    if (handler.succeeded()) {
                        promise.complete(handler.result().getSourceType());
                    } else {
                        promise.fail(handler.cause());
                    }
                });
        assertTrue(promise.future().succeeded());
        assertEquals(SourceType.INFORMATION_SCHEMA, promise.future().result());
    }

    @Test
    void getMultiSchemasError() {
        Promise promise = Promise.promise();
        QuerySourceRequest sourceRequest = new QuerySourceRequest();
        final QueryRequest queryRequest = new QueryRequest(UUID.randomUUID(),
                "dtm",
                "select * from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = 'DEMO'");
        queryRequest.setDeltaInformations(Arrays.asList(
                DeltaInformation.builder()
                        .schemaName("dtm")
                        .build(),
                DeltaInformation.builder()
                        .schemaName("dtm_707")
                        .build()));
        sourceRequest.setQueryRequest(queryRequest);

        informationSchemaDefinitionService.tryGetInformationSchemaRequest(sourceRequest)
                .onComplete(handler -> {
                    if (handler.succeeded()) {
                        promise.complete(handler.result().getSourceType());
                    } else {
                        promise.fail(handler.cause());
                    }
                });
        assertTrue(promise.future().failed());
    }

    @Test
    void getNotInfoSchemaRequestSuccess() {
        Promise promise = Promise.promise();
        QuerySourceRequest sourceRequest = new QuerySourceRequest();
        final QueryRequest queryRequest = new QueryRequest(UUID.randomUUID(),
                "dtm_707",
                "select * from test;");
        queryRequest.setDeltaInformations(Collections.singletonList(
                DeltaInformation.builder()
                        .schemaName("dtm_707")
                        .build()
        ));
        sourceRequest.setQueryRequest(queryRequest);

        informationSchemaDefinitionService.tryGetInformationSchemaRequest(sourceRequest)
                .onComplete(handler -> {
                    if (handler.succeeded()) {
                        promise.complete(handler.result().getSourceType());
                    } else {
                        promise.fail(handler.cause());
                    }
                });
        assertTrue(promise.future().succeeded());
        assertEquals(sourceRequest.getSourceType(), promise.future().result());
    }

}