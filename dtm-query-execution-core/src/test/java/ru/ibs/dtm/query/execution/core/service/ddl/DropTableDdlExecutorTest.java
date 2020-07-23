package ru.ibs.dtm.query.execution.core.service.ddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.model.ddl.ClassTypes;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.ibs.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import ru.ibs.dtm.query.execution.core.configuration.jooq.MariaProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import ru.ibs.dtm.query.execution.core.dao.servicedb.AttributeDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.DatamartDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.EntityDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.ServiceDbDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.impl.AttributeDaoImpl;
import ru.ibs.dtm.query.execution.core.dao.servicedb.impl.DatamartDaoImpl;
import ru.ibs.dtm.query.execution.core.dao.servicedb.impl.EntityDaoImpl;
import ru.ibs.dtm.query.execution.core.dao.servicedb.impl.ServiceDbDaoImpl;
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

    private CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private Planner planner;
    private final MetadataExecutor<DdlRequestContext> metadataExecutor = mock(MetadataExecutorImpl.class);
    private final MariaProperties mariaProperties = mock(MariaProperties.class);
    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDaoImpl.class);
    private final DatamartDao datamartDao = mock(DatamartDaoImpl.class);
    private final EntityDao entityDao = mock(EntityDaoImpl.class);
    private final AttributeDao attributeDao = mock(AttributeDaoImpl.class);
    private QueryResultDdlExecutor dropTableDdlExecutor;
    private DdlRequestContext context;
    private SqlNode query;
    private ClassTable classTable;
    private Long datamartId;
    private Long entityId;
    private String schema;

    @BeforeEach
    void setUp() throws SqlParseException {
        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        planner = Frameworks.getPlanner(frameworkConfig);
        dropTableDdlExecutor = new DropTableDdlExecutor(metadataExecutor,
                mariaProperties, serviceDbFacade);
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        when(serviceDbDao.getAttributeDao()).thenReturn(attributeDao);

        schema = "shares";
        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setSubRequestId(UUID.randomUUID().toString());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql("drop table accounts");
        query = planner.parse(queryRequest.getSql());
        context = new DdlRequestContext(new DdlRequest(queryRequest));
        context.getRequest().setQueryRequest(queryRequest);
        context.setQuery(query);
        datamartId = 1L;
        entityId = 1L;
        ClassField f1 = new ClassField(0, "id", ClassTypes.INT, false, true);
        ClassField f2 = new ClassField(1, "name", ClassTypes.VARCHAR, true, false);
        f2.setSize(100);
        String sqlNodeName = "accounts";
        classTable = new ClassTable(sqlNodeName, schema, Arrays.asList(f1, f2));
        context.getRequest().setClassTable(classTable);
        context.setDatamartId(datamartId);
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
        }).when(entityDao).dropEntity(eq(datamartId),eq(context.getRequest().getClassTable().getName()), any());

        dropTableDdlExecutor.execute(context, context.getRequest().getClassTable().getName(), ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().result());
    }

    @Test
    void executeWithFindDatamartError() {
        Promise promise = Promise.promise();
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(datamartDao).findDatamart(eq(schema), any());

        dropTableDdlExecutor.execute(context, context.getRequest().getClassTable().getName(), ar -> {
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
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(datamartId));
            return null;
        }).when(datamartDao).findDatamart(eq(schema), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(2);
            handler.handle(Future.failedFuture(new RuntimeException("")));
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
        }).when(entityDao).dropEntity(eq(datamartId),eq(context.getRequest().getClassTable().getName()), any());

        dropTableDdlExecutor.execute(context, context.getRequest().getClassTable().getName(), ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().result());
    }

    @Test
    void executeWithFindTableError() {
        Promise promise = Promise.promise();
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(datamartId));
            return null;
        }).when(datamartDao).findDatamart(eq(schema), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(2);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(entityDao).findEntity(eq(context.getDatamartId()),
                eq(context.getRequest().getClassTable().getName()), any());

        dropTableDdlExecutor.execute(context, context.getRequest().getClassTable().getName(), ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }

    @Test
    void executeWithMetadataExecuteError() {
        Promise promise = Promise.promise();
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(datamartId));
            return null;
        }).when(datamartDao).findDatamart(eq(schema), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture(1L));
            return null;
        }).when(entityDao).findEntity(eq(context.getDatamartId()),
                eq(context.getRequest().getClassTable().getName()), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new RuntimeException()));
            return null;
        }).when(metadataExecutor).execute(any(), any());

        dropTableDdlExecutor.execute(context, context.getRequest().getClassTable().getName(), ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }

    @Test
    void executeWithDropAttributeError() {
        Promise promise = Promise.promise();
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(datamartId));
            return null;
        }).when(datamartDao).findDatamart(eq(schema), any());

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
            handler.handle(Future.failedFuture(new RuntimeException()));
            return null;
        }).when(attributeDao).dropAttribute(any(), any());

        dropTableDdlExecutor.execute(context, context.getRequest().getClassTable().getName(), ar -> {
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
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(datamartId));
            return null;
        }).when(datamartDao).findDatamart(eq(schema), any());

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
            handler.handle(Future.failedFuture(new RuntimeException()));
            return null;
        }).when(entityDao).dropEntity(eq(datamartId),eq(context.getRequest().getClassTable().getName()), any());

        dropTableDdlExecutor.execute(context, context.getRequest().getClassTable().getName(), ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }
}
