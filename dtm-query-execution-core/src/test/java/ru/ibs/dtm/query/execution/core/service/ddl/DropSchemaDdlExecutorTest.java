package ru.ibs.dtm.query.execution.core.service.ddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.model.ddl.ColumnType;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.ibs.dtm.query.calcite.core.framework.DtmCalciteFramework;
import ru.ibs.dtm.query.calcite.core.service.DefinitionService;
import ru.ibs.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import ru.ibs.dtm.query.execution.core.configuration.jooq.MariaProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import ru.ibs.dtm.query.execution.core.dao.servicedb.*;
import ru.ibs.dtm.query.execution.core.dao.servicedb.impl.*;
import ru.ibs.dtm.query.execution.core.dto.metadata.DatamartEntity;
import ru.ibs.dtm.query.execution.core.service.ddl.impl.DropSchemaDdlExecutor;
import ru.ibs.dtm.query.execution.core.service.impl.CoreCalciteDefinitionService;
import ru.ibs.dtm.query.execution.core.service.metadata.MetadataExecutor;
import ru.ibs.dtm.query.execution.core.service.metadata.impl.MetadataExecutorImpl;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DdlRequest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DropSchemaDdlExecutorTest {

    private CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private Planner planner;
    private final DefinitionService<SqlNode> definitionService =
            new CoreCalciteDefinitionService(calciteConfiguration.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory()));
    private final MetadataExecutor<DdlRequestContext> metadataExecutor = mock(MetadataExecutorImpl.class);
    private final MariaProperties mariaProperties = mock(MariaProperties.class);
    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDaoImpl.class);
    private final DatamartDao datamartDao = mock(DatamartDaoImpl.class);
    private final EntityDao entityDao = mock(EntityDaoImpl.class);
    private final AttributeDao attributeDao = mock(AttributeDaoImpl.class);
    private final ViewDao viewDao = mock(ViewDaoImpl.class);
    private DropSchemaDdlExecutor dropSchemaDdlExecutor;
    private DdlRequestContext context;
    private List<DatamartEntity> entities;
    private SqlNode dropSchemaQuery;
    private ClassTable classTable;
    private Long datamartId;
    private Long entityId;
    private String schema;

    @BeforeEach
    void setUp() throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        dropSchemaDdlExecutor = new DropSchemaDdlExecutor(metadataExecutor,
                mariaProperties, serviceDbFacade, definitionService);
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        when(serviceDbDao.getViewServiceDao()).thenReturn(viewDao);
        when(serviceDbDao.getAttributeDao()).thenReturn(attributeDao);
        schema = "shares";
        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setSubRequestId(UUID.randomUUID().toString());
        queryRequest.setDatamartMnemonic(schema);
        final String dropSchemaSql = "drop database shares";
        queryRequest.setSql(dropSchemaSql);
        dropSchemaQuery = planner.parse(queryRequest.getSql());
        context = new DdlRequestContext(new DdlRequest(queryRequest));
        context.getRequest().setQueryRequest(queryRequest);
        context.setQuery(dropSchemaQuery);
        context.setDatamartName(schema);
        datamartId = 1L;
        entityId = 1L;
        ClassField f1 = new ClassField(0, "id", ColumnType.INT, false, true);
        ClassField f2 = new ClassField(1, "name", ColumnType.VARCHAR, true, false);
        f2.setSize(100);
        String table = "accounts";
        classTable = new ClassTable(table, schema, Arrays.asList(f1, f2));
        context.getRequest().setClassTable(classTable);
        context.setDatamartId(datamartId);
        entities = new ArrayList<>();
        DatamartEntity entity = new DatamartEntity(1L, table, schema);
        entities.add(entity);
    }

    @Test
    void executeSuccess() {
        Promise promise = Promise.promise();
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(datamartId));
            return null;
        }).when(datamartDao).findDatamart(eq(schema), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<DatamartEntity>>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(entities));
            return null;
        }).when(entityDao).getEntitiesMeta(eq(context.getDatamartName()), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(viewDao).dropViewsByDatamartId(eq(context.getDatamartId()), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(datamartDao).dropDatamart(eq(context.getDatamartId()), any());

        dropTables();

        dropSchemaDdlExecutor.execute(context, null, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().result());
    }

    @Test
    void executeWithGetEntitiesError() {
        Promise promise = Promise.promise();
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(datamartId));
            return null;
        }).when(datamartDao).findDatamart(eq(schema), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<DatamartEntity>>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(entityDao).getEntitiesMeta(eq(context.getDatamartName()), any());

        dropSchemaDdlExecutor.execute(context, null, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }

    @Test
    void executeWithDropViewsError() {
        Promise promise = Promise.promise();
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(datamartId));
            return null;
        }).when(datamartDao).findDatamart(eq(schema), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<DatamartEntity>>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(entities));
            return null;
        }).when(entityDao).getEntitiesMeta(eq(context.getDatamartName()), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(viewDao).dropViewsByDatamartId(eq(context.getDatamartId()), any());

        dropTables();

        dropSchemaDdlExecutor.execute(context, null, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }

    @Test
    void executeWithDropDatamartError() {
        Promise promise = Promise.promise();
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(datamartId));
            return null;
        }).when(datamartDao).findDatamart(eq(schema), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<DatamartEntity>>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(entities));
            return null;
        }).when(entityDao).getEntitiesMeta(eq(context.getDatamartName()), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(viewDao).dropViewsByDatamartId(eq(datamartId), any());

        dropTables();

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(datamartDao).dropDatamart(any(), any());

        dropSchemaDdlExecutor.execute(context, null, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }

    private void dropTables() {
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture(entityId));
            return null;
        }).when(entityDao).findEntity(eq(context.getDatamartId()),
                eq(context.getRequest().getClassTable().getName()), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(metadataExecutor).execute(any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(attributeDao).dropAttribute(any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Integer>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture(0));
            return null;
        }).when(entityDao).dropEntity(eq(datamartId), eq(context.getRequest().getClassTable().getName()), any());
    }
}
