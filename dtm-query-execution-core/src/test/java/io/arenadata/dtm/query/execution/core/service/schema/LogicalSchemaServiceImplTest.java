package io.arenadata.dtm.query.execution.core.service.schema;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.dto.schema.DatamartSchemaKey;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.EntityDaoImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.ServiceDbDaoImpl;
import io.arenadata.dtm.query.execution.core.service.schema.impl.LogicalSchemaServiceImpl;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LogicalSchemaServiceImplTest {

    public static final String DATAMART = "test_datamart";
    public static final String TABLE_PSO = "pso";
    public static final String TABLE_DOC = "doc";
    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDaoImpl.class);
    private final EntityDao entityDao = mock(EntityDaoImpl.class);
    private LogicalSchemaService logicalSchemaService;
    private QueryRequest queryRequest;

    @BeforeEach
    void setUp() {
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        logicalSchemaService = new LogicalSchemaServiceImpl(serviceDbFacade);
        queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic(DATAMART);
        queryRequest.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        queryRequest.setDeltaInformations(Arrays.asList(
                DeltaInformation.builder()
                        .schemaName(DATAMART)
                        .tableName("pso")
                        .build(),
                DeltaInformation.builder()
                        .schemaName(DATAMART)
                        .tableName("doc")
                        .build()
        ));
    }

    @Test
    void createSchemaSuccess() {
        Promise<Map<DatamartSchemaKey, Entity>> promise = Promise.promise();
        final Map<DatamartSchemaKey, Entity> resultSchemaMap = new HashMap<>();
        queryRequest.setSql("select t1.id, cast(t2.id as varchar(10)) as tt from test_datamart.pso t1 \n" +
                " join test_datamart.doc t2 on t1.id = t2.id");
        Entity pso = Entity.builder()
                .schema(DATAMART)
                .name(TABLE_PSO)
                .build();

        EntityField entityField = EntityField.builder()
                .name("id")
                .type(ColumnType.INT)
                .ordinalPosition(0)
                .shardingOrder(1)
                .nullable(false)
                .primaryOrder(1)
                .accuracy(0)
                .size(0)
                .build();
        List<EntityField> psoAttrs = Collections.singletonList(entityField);
        pso.setFields(psoAttrs);

        Entity doc = Entity.builder()
                .schema(DATAMART)
                .name(TABLE_DOC)
                .build();
        List<EntityField> docAttrs = Collections.singletonList(entityField);
        doc.setFields(docAttrs);

        resultSchemaMap.put(new DatamartSchemaKey(DATAMART, TABLE_PSO), pso);
        resultSchemaMap.put(new DatamartSchemaKey(DATAMART, TABLE_DOC), doc);

        Entity.EntityBuilder builder = Entity.builder()
                .schema(DATAMART)
                .entityType(EntityType.TABLE)
                .fields(Collections.singletonList(
                        EntityField.builder()
                                .name("id")
                                .accuracy(0)
                                .size(0)
                                .ordinalPosition(0)
                                .nullable(false)
                                .shardingOrder(1)
                                .primaryOrder(1)
                                .type(ColumnType.INT)
                                .build()
                ));

        when(entityDao.getEntity(any(), any()))
                .thenReturn(
                        Future.succeededFuture(
                                builder
                                        .name(TABLE_PSO)
                                        .build()),
                        Future.succeededFuture(
                                builder
                                        .name(TABLE_DOC)
                                        .build())
                );
        logicalSchemaService.createSchema(queryRequest)
                .onComplete(promise);
        Map<DatamartSchemaKey, Entity> schemaMap = promise.future().result();
        assertNotNull(schemaMap);
        schemaMap.forEach((k, v) -> {
            assertEquals(resultSchemaMap.get(k).getName(), v.getName());
            assertEquals(resultSchemaMap.get(k).getName(), v.getName());
            assertEquals(resultSchemaMap.get(k).getFields().get(0).getName(), v.getFields().get(0).getName());
            assertEquals(resultSchemaMap.get(k).getFields().get(0).getType(), v.getFields().get(0).getType());
            assertEquals(resultSchemaMap.get(k).getFields().get(0).getPrimaryOrder(), v.getFields().get(0).getPrimaryOrder());
            assertEquals(resultSchemaMap.get(k).getFields().get(0).getShardingOrder(), v.getFields().get(0).getShardingOrder());
        });
    }

    @Test
    void createSchemaWithDatamartEntityError() {
        Promise<Map<DatamartSchemaKey, Entity>> promise = Promise.promise();
        queryRequest.setSql("select t1.id, cast(t2.id as varchar(10)) as tt from test_datamart.pso t1 \n" +
                " join test_datamart.doc t2 on t1.id = t2.id");

        Mockito.when(entityDao.getEntity(any(), any()))
                .thenReturn(Future.failedFuture(new DtmException("Error getting entities!")));

        logicalSchemaService.createSchema(queryRequest)
                .onComplete(promise);
        assertNotNull(promise.future().cause());
    }

}
