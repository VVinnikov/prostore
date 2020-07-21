package ru.ibs.dtm.query.execution.core.service.schema;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import java.util.*;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.dto.DatamartInfo;
import ru.ibs.dtm.common.dto.schema.DatamartSchemaKey;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.ibs.dtm.query.calcite.core.service.DefinitionService;
import ru.ibs.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import ru.ibs.dtm.query.execution.core.dao.servicedb.AttributeDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.EntityDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.ServiceDbDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.impl.AttributeDaoImpl;
import ru.ibs.dtm.query.execution.core.dao.servicedb.impl.EntityDaoImpl;
import ru.ibs.dtm.query.execution.core.dao.servicedb.impl.ServiceDbDaoImpl;
import ru.ibs.dtm.query.execution.core.dto.metadata.DatamartEntity;
import ru.ibs.dtm.query.execution.core.dto.metadata.EntityAttribute;
import ru.ibs.dtm.query.execution.core.service.impl.CoreCalciteDefinitionService;
import ru.ibs.dtm.query.execution.model.metadata.AttributeType;
import ru.ibs.dtm.query.execution.model.metadata.ColumnType;
import ru.ibs.dtm.query.execution.model.metadata.DatamartTable;
import ru.ibs.dtm.query.execution.model.metadata.TableAttribute;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class LogicalSchemaServiceImplTest {

    private final CalciteConfiguration config = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final DefinitionService<SqlNode> definitionService =
            new CoreCalciteDefinitionService(config.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory()));
    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDaoImpl.class);
    private final EntityDao entityDao = mock(EntityDaoImpl.class);
    private final AttributeDao attributeDao = mock(AttributeDaoImpl.class);
    private LogicalSchemaService logicalSchemaService;
    private QueryRequest queryRequest;

    @BeforeEach
    void setUp() {
        logicalSchemaService = new LogicalSchemaServiceImpl(serviceDbFacade, definitionService);
        queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic("test_datamart");
        queryRequest.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        queryRequest.setSubRequestId("6efad624-b9da-4ba1-9fed-f2da478b08e8");

        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        when(serviceDbDao.getAttributeDao()).thenReturn(attributeDao);
    }

    @Test
    void createSchemaSuccess() {
        Promise promise = Promise.promise();
        final Map<DatamartSchemaKey, DatamartTable> resultSchemaMap = new HashMap<>();
        queryRequest.setSql("select t1.id, cast(t2.id as varchar(10)) as tt from test_datamart.pso t1 \n" +
                " join test_datamart.doc t2 on t1.id = t2.id");
        DatamartTable pso = new DatamartTable();
        pso.setLabel("pso");
        pso.setMnemonic("test_datamart");
        List<TableAttribute> psoAttrs = Arrays.asList(new TableAttribute(UUID.randomUUID(), "id",
                new AttributeType(UUID.randomUUID(), ColumnType.INT), 0, 0, 1, null));
        pso.setTableAttributes(psoAttrs);
        DatamartTable doc = new DatamartTable();
        doc.setLabel("doc");
        doc.setMnemonic("test_datamart");
        List<TableAttribute> docAttrs = Arrays.asList(new TableAttribute(UUID.randomUUID(), "id",
                new AttributeType(UUID.randomUUID(), ColumnType.INT), 0, 0, null, 1));
        doc.setTableAttributes(docAttrs);
        resultSchemaMap.put(new DatamartSchemaKey("test_datamart", "pso"), pso);
        resultSchemaMap.put(new DatamartSchemaKey("test_datamart", "doc"), doc);

        final DatamartInfo datamartInfo = new DatamartInfo("test_datamart", new HashSet<>(Arrays.asList("pso", "doc")));
        final List<DatamartEntity> entities = new ArrayList<>();
        entities.add(new DatamartEntity(1, "pso", "test_datamart"));
        entities.add(new DatamartEntity(2, "doc", "test_datamart"));

        final List<EntityAttribute> psoAttributes = new ArrayList<>();
        psoAttributes.add(new EntityAttribute(1, "id", "integer",
                0, 0, "pso", "test_datamart", 1, null));
        final List<EntityAttribute> docAttributes = new ArrayList<>();
        docAttributes.add(new EntityAttribute(1, "id", "integer",
                0, 0, "doc", "test_datamart", null, 1));

        doAnswer(invocation -> {
            final Handler<AsyncResult<List<DatamartEntity>>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(entities));
            return null;
        }).when(entityDao).findEntitiesByDatamartAndTableNames(eq(datamartInfo), any());

        doAnswer(invocation -> {
            final Handler<AsyncResult<List<EntityAttribute>>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture(psoAttributes));
            return null;
        }).when(attributeDao).getAttributesMeta(eq(entities.get(0).getDatamartMnemonic()),
                eq(entities.get(0).getMnemonic()), any());

        doAnswer(invocation -> {
            final Handler<AsyncResult<List<EntityAttribute>>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture(docAttributes));
            return null;
        }).when(attributeDao).getAttributesMeta(eq(entities.get(1).getDatamartMnemonic()),
                eq(entities.get(1).getMnemonic()), any());

        logicalSchemaService.createSchema(queryRequest, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        Map<DatamartSchemaKey, DatamartTable> schemaMap = (Map<DatamartSchemaKey, DatamartTable>) promise.future().result();
        assertNotNull(schemaMap);
        schemaMap.forEach((k, v) -> {
            assertEquals(resultSchemaMap.get(k).getLabel(), v.getLabel());
            assertEquals(resultSchemaMap.get(k).getMnemonic(), v.getMnemonic());
            assertEquals(resultSchemaMap.get(k).getTableAttributes().get(0).getMnemonic(), v.getTableAttributes().get(0).getMnemonic());
            assertEquals(resultSchemaMap.get(k).getTableAttributes().get(0).getType().getValue(), v.getTableAttributes().get(0).getType().getValue());
            assertEquals(resultSchemaMap.get(k).getTableAttributes().get(0).getPrimaryKeyOrder(), v.getTableAttributes().get(0).getPrimaryKeyOrder());
            assertEquals(resultSchemaMap.get(k).getTableAttributes().get(0).getDistributeKeyOrder(), v.getTableAttributes().get(0).getDistributeKeyOrder());
        });
    }

    @Test
    void createSchemaWithDatamartEntityError() {
        Promise promise = Promise.promise();
        queryRequest.setSql("select t1.id, cast(t2.id as varchar(10)) as tt from test_datamart.pso t1 \n" +
                " join test_datamart.doc t2 on t1.id = t2.id");

        doAnswer(invocation -> {
            final Handler<AsyncResult<List<DatamartEntity>>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new RuntimeException("Ошибка получения entities!")));
            return null;
        }).when(entityDao).findEntitiesByDatamartAndTableNames(any(), any());

        logicalSchemaService.createSchema(queryRequest, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }

    @Test
    void createSchemaWithTableAttributeError() {
        Promise promise = Promise.promise();
        queryRequest.setSql("select t1.id, cast(t2.id as varchar(10)) as tt from test_datamart.pso t1 \n" +
                " join test_datamart.doc t2 on t1.id = t2.id");
        final List<DatamartEntity> entities = new ArrayList<>();
        entities.add(new DatamartEntity(1, "pso", "test_datamart"));
        entities.add(new DatamartEntity(2, "doc", "test_datamart"));
        final List<EntityAttribute> psoAttributes = new ArrayList<>();
        psoAttributes.add(new EntityAttribute(1, "id", "integer",
                0, 0, "pso", "test_datamart", null, null));

        doAnswer(invocation -> {
            final Handler<AsyncResult<List<DatamartEntity>>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(entities));
            return null;
        }).when(entityDao).findEntitiesByDatamartAndTableNames(any(), any());

        doAnswer(invocation -> {
            final Handler<AsyncResult<List<EntityAttribute>>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture(psoAttributes));
            return null;
        }).when(attributeDao).getAttributesMeta(eq(entities.get(0).getDatamartMnemonic()),
                eq(entities.get(0).getMnemonic()), any());

        doAnswer(invocation -> {
            final Handler<AsyncResult<List<EntityAttribute>>> handler = invocation.getArgument(2);
            handler.handle(Future.failedFuture(new RuntimeException("Ошибка получения атрибутов таблицы!")));
            return null;
        }).when(attributeDao).getAttributesMeta(eq(entities.get(1).getDatamartMnemonic()),
                eq(entities.get(1).getMnemonic()), any());

        logicalSchemaService.createSchema(queryRequest, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }
}