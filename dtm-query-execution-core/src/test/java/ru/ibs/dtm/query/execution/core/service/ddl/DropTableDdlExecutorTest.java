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
import ru.ibs.dtm.common.model.ddl.ColumnType;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.common.model.ddl.EntityField;
import ru.ibs.dtm.common.model.ddl.EntityType;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.ibs.dtm.query.calcite.core.framework.DtmCalciteFramework;
import ru.ibs.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import ru.ibs.dtm.query.execution.core.dao.exception.entity.EntityNotExistsException;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.ServiceDbDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.impl.DatamartDaoImpl;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.impl.EntityDaoImpl;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.impl.ServiceDbDaoImpl;
import ru.ibs.dtm.query.execution.core.service.ddl.impl.DropTableDdlExecutor;
import ru.ibs.dtm.query.execution.core.service.metadata.MetadataExecutor;
import ru.ibs.dtm.query.execution.core.service.metadata.impl.MetadataExecutorImpl;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DdlRequest;

import java.util.Arrays;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DropTableDdlExecutorTest {

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private final MetadataExecutor<DdlRequestContext> metadataExecutor = mock(MetadataExecutorImpl.class);
    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDaoImpl.class);
    private final DatamartDao datamartDao = mock(DatamartDaoImpl.class);
    private final EntityDao entityDao = mock(EntityDaoImpl.class);
    private QueryResultDdlExecutor dropTableDdlExecutor;
    private DdlRequestContext context;
    private String schema;

    @BeforeEach
    void setUp() throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        dropTableDdlExecutor = new DropTableDdlExecutor(metadataExecutor, serviceDbFacade);

        schema = "shares";
        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setSubRequestId(UUID.randomUUID().toString());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql("drop table accounts");
        SqlNode query = planner.parse(queryRequest.getSql());
        context = new DdlRequestContext(new DdlRequest(queryRequest));
        context.getRequest().setQueryRequest(queryRequest);
        context.setQuery(query);
        EntityField f1 = new EntityField(0, "id", ColumnType.INT, false);
        f1.setPrimaryOrder(1);
        f1.setShardingOrder(1);
        EntityField f2 = new EntityField(1, "name", ColumnType.VARCHAR, true);
        f2.setSize(100);
        String sqlNodeName = "accounts";
        Entity entity = new Entity(sqlNodeName, schema, Arrays.asList(f1, f2));
        entity.setEntityType(EntityType.TABLE);
        context.getRequest().setEntity(entity);
    }

    @Test
    void executeSuccess() {
        Promise promise = Promise.promise();

        Mockito.when(entityDao.getEntity(eq(schema), eq(context.getRequest().getEntity().getName())))
            .thenReturn(Future.succeededFuture(context.getRequest().getEntity()));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(metadataExecutor).execute(any(), any());

        Mockito.when(entityDao.deleteEntity(eq(schema), eq(context.getRequest().getEntity().getName())))
            .thenReturn(Future.succeededFuture());

        dropTableDdlExecutor.execute(context, context.getRequest().getEntity().getName(), ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().result());
    }

    @Test
    void executeWithFindView() {
        Promise promise = Promise.promise();

        Mockito.when(entityDao.getEntity(eq(schema), eq(context.getRequest().getEntity().getName())))
            .thenReturn(Future.succeededFuture(Entity.builder()
                .schema(schema)
                .name(context.getRequest().getEntity().getName())
                .entityType(EntityType.VIEW)
                .build()));

        dropTableDdlExecutor.execute(context, context.getRequest().getEntity().getName(), ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }

    @Test
    void executeWithIfExistsStmtSuccess() {
        Promise promise = Promise.promise();
        context.getRequest().getQueryRequest().setSql("DROP TABLE IF EXISTS accounts");
        String entityName = context.getRequest().getEntity().getName();
        Mockito.when(entityDao.getEntity(eq(schema), eq(entityName)))
            .thenReturn(Future.failedFuture(new EntityNotExistsException(entityName)));

        dropTableDdlExecutor.execute(context, entityName, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().result());
    }

    @Test
    void executeWithMetadataExecuteError() {
        Promise promise = Promise.promise();
        Mockito.when(entityDao.getEntity(eq(schema), eq(context.getRequest().getEntity().getName())))
            .thenReturn(Future.succeededFuture(context.getRequest().getEntity()));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new RuntimeException()));
            return null;
        }).when(metadataExecutor).execute(any(), any());

        dropTableDdlExecutor.execute(context, context.getRequest().getEntity().getName(), ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }

    @Test
    void executeWithDropEntityError() {
        Promise promise = Promise.promise();
        Mockito.when(entityDao.getEntity(eq(schema), eq(context.getRequest().getEntity().getName())))
            .thenReturn(Future.succeededFuture(context.getRequest().getEntity()));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(metadataExecutor).execute(any(), any());

        Mockito.when(entityDao.deleteEntity(eq(schema), eq(context.getRequest().getEntity().getName())))
            .thenReturn(Future.failedFuture("delete entity error"));

        dropTableDdlExecutor.execute(context, context.getRequest().getEntity().getName(), ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }
}
