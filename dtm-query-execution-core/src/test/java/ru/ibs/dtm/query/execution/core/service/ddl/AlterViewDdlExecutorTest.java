package ru.ibs.dtm.query.execution.core.service.ddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.mysqlclient.MySQLConnectOptions;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.ibs.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import ru.ibs.dtm.query.execution.core.configuration.jooq.MariaProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import ru.ibs.dtm.query.execution.core.dao.servicedb.DatamartDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.EntityDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.ServiceDbDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.ViewDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.impl.DatamartDaoImpl;
import ru.ibs.dtm.query.execution.core.dao.servicedb.impl.EntityDaoImpl;
import ru.ibs.dtm.query.execution.core.dao.servicedb.impl.ServiceDbDaoImpl;
import ru.ibs.dtm.query.execution.core.dao.servicedb.impl.ViewDaoImpl;
import ru.ibs.dtm.query.execution.core.service.ddl.impl.AlterViewDdlExecutor;
import ru.ibs.dtm.query.execution.core.service.metadata.MetadataExecutor;
import ru.ibs.dtm.query.execution.core.service.metadata.impl.MetadataExecutorImpl;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DdlRequest;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AlterViewDdlExecutorTest {

    private CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(
            calciteCoreConfiguration.eddlParserImplFactory());
    private final MariaProperties mariaProperties = mock(MariaProperties.class);
    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDaoImpl.class);
    private final EntityDao entityDao = mock(EntityDaoImpl.class);
    private final ViewDao viewDao = mock(ViewDaoImpl.class);
    private final DatamartDao datamartDao = mock(DatamartDaoImpl.class);
    private final MetadataExecutor<DdlRequestContext> metadataExecutor = mock(MetadataExecutorImpl.class);
    private AlterViewDdlExecutor alterViewDdlExecutor;
    private DdlRequestContext context;
    private String schema;
    private SqlNode query;
    private Planner planner;
    private Long datamartId;
    private String viewName;
    private String sqlNodeName;

    @BeforeEach
    void setUp() throws SqlParseException {
        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        planner = Frameworks.getPlanner(frameworkConfig);
        alterViewDdlExecutor = new AlterViewDdlExecutor(metadataExecutor,
                mariaProperties, serviceDbFacade, new SqlDialect(SqlDialect.EMPTY_CONTEXT));
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        when(serviceDbDao.getViewServiceDao()).thenReturn(viewDao);
        when(mariaProperties.getOptions()).thenReturn(
                new MySQLConnectOptions()
                        .setPort(3306)
                        .setHost("localhost")
                        .setDatabase("servicedb")
                        .setUser("")
                        .setPassword("")
        );
        schema = "shares";
        viewName = "test_view";
        sqlNodeName = schema + "." + viewName;
        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setSubRequestId(UUID.randomUUID().toString());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql(String.format("ALTER VIEW %s.test_view AS SELECT * FROM %s.test_table",
                schema, schema));
        query = planner.parse(queryRequest.getSql());
        context = new DdlRequestContext(new DdlRequest(queryRequest));
        datamartId = 1L;
        context.getRequest().setQueryRequest(queryRequest);
        context.setQuery(query);
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
            final Handler<AsyncResult<Boolean>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture(false));
            return null;
        }).when(entityDao).isEntityExists(eq(datamartId), eq(viewName), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Boolean>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture(true));
            return null;
        }).when(viewDao).existsView(eq(viewName), eq(datamartId), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(3);
            handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
            return null;
        }).when(viewDao).updateView(eq(viewName), eq(datamartId), any(), any());

        alterViewDdlExecutor.execute(context, sqlNodeName, ar -> {
                    if (ar.succeeded()) {
                        promise.complete(ar.result());
                    } else {
                        promise.fail(ar.cause());
                    }
                }
        );
        assertNotNull(promise.future().result());
    }

    @Test
    void executeWithFindDatamartError() {
        Promise promise = Promise.promise();
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new RuntimeException("Error!")));
            return null;
        }).when(datamartDao).findDatamart(eq(schema), any());

        alterViewDdlExecutor.execute(context, sqlNodeName, ar -> {
                    if (ar.succeeded()) {
                        promise.complete(ar.result());
                    } else {
                        promise.fail(ar.cause());
                    }
                }
        );
        assertTrue(promise.future().failed());
    }

    @Test
    void executeIsEntityExistsTrue() {
        Promise promise = Promise.promise();
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(datamartId));
            return null;
        }).when(datamartDao).findDatamart(eq(schema), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Boolean>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture(true));
            return null;
        }).when(entityDao).isEntityExists(eq(datamartId), eq(viewName), any());

        alterViewDdlExecutor.execute(context, sqlNodeName, ar -> {
                    if (ar.succeeded()) {
                        promise.complete(ar.result());
                    } else {
                        promise.fail(ar.cause());
                    }
                }
        );
        assertTrue(promise.future().failed());
    }

    @Test
    void executeIsEntityExistsError() {
        Promise promise = Promise.promise();
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(datamartId));
            return null;
        }).when(datamartDao).findDatamart(eq(schema), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Boolean>> handler = invocation.getArgument(2);
            handler.handle(Future.failedFuture(new RuntimeException("Error!")));
            return null;
        }).when(entityDao).isEntityExists(eq(datamartId), eq(viewName), any());

        alterViewDdlExecutor.execute(context, sqlNodeName, ar -> {
                    if (ar.succeeded()) {
                        promise.complete(ar.result());
                    } else {
                        promise.fail(ar.cause());
                    }
                }
        );
        assertTrue(promise.future().failed());
    }

    @Test
    void executeWithViewExistsFalse() {
        Promise promise = Promise.promise();
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(datamartId));
            return null;
        }).when(datamartDao).findDatamart(eq(schema), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Boolean>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture(false));
            return null;
        }).when(entityDao).isEntityExists(eq(datamartId), eq(viewName), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Boolean>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture(false));
            return null;
        }).when(viewDao).existsView(eq(viewName), eq(datamartId), any());

        alterViewDdlExecutor.execute(context, sqlNodeName, ar -> {
                    if (ar.succeeded()) {
                        promise.complete(ar.result());
                    } else {
                        promise.fail(ar.cause());
                    }
                }
        );
        assertTrue(promise.future().failed());
    }

    @Test
    void executeWithViewExistsError() {
        Promise promise = Promise.promise();
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(datamartId));
            return null;
        }).when(datamartDao).findDatamart(eq(schema), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Boolean>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture(false));
            return null;
        }).when(entityDao).isEntityExists(eq(datamartId), eq(viewName), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Boolean>> handler = invocation.getArgument(2);
            handler.handle(Future.failedFuture(new RuntimeException("Error!")));
            return null;
        }).when(viewDao).existsView(eq(viewName), eq(datamartId), any());

        alterViewDdlExecutor.execute(context, sqlNodeName, ar -> {
                    if (ar.succeeded()) {
                        promise.complete(ar.result());
                    } else {
                        promise.fail(ar.cause());
                    }
                }
        );
        assertTrue(promise.future().failed());
    }

    @Test
    void executeWithViewUpdateError() {
        Promise promise = Promise.promise();
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(datamartId));
            return null;
        }).when(datamartDao).findDatamart(eq(schema), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Boolean>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture(false));
            return null;
        }).when(entityDao).isEntityExists(eq(datamartId), eq(viewName), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Boolean>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture(true));
            return null;
        }).when(viewDao).existsView(eq(viewName), eq(datamartId), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(3);
            handler.handle(Future.failedFuture(new RuntimeException("Error!")));
            return null;
        }).when(viewDao).updateView(eq(viewName), eq(datamartId), any(), any());

        alterViewDdlExecutor.execute(context, sqlNodeName, ar -> {
                    if (ar.succeeded()) {
                        promise.complete(ar.result());
                    } else {
                        promise.fail(ar.cause());
                    }
                }
        );
        assertTrue(promise.future().failed());
    }
}
