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
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.ibs.dtm.query.calcite.core.framework.DtmCalciteFramework;
import ru.ibs.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import ru.ibs.dtm.query.execution.core.configuration.jooq.MariaProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import ru.ibs.dtm.query.execution.core.dao.servicedb.DatamartDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.ServiceDbDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.impl.DatamartDaoImpl;
import ru.ibs.dtm.query.execution.core.dao.servicedb.impl.ServiceDbDaoImpl;
import ru.ibs.dtm.query.execution.core.service.ddl.impl.CreateSchemaDdlExecutor;
import ru.ibs.dtm.query.execution.core.service.metadata.MetadataExecutor;
import ru.ibs.dtm.query.execution.core.service.metadata.impl.MetadataExecutorImpl;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DdlRequest;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CreateSchemaDdlExecutorTest {

    private CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private Planner planner;
    private final MetadataExecutor<DdlRequestContext> metadataExecutor = mock(MetadataExecutorImpl.class);
    private final MariaProperties mariaProperties = mock(MariaProperties.class);
    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDaoImpl.class);
    private final DatamartDao datamartDao = mock(DatamartDaoImpl.class);
    private QueryResultDdlExecutor createSchemaDdlExecutor;
    private DdlRequestContext context;
    private SqlNode query;
    private String schema;
    private Long datamartId;

    @BeforeEach
    void setUp() throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        createSchemaDdlExecutor = new CreateSchemaDdlExecutor(metadataExecutor,
                mariaProperties, serviceDbFacade);
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        datamartId = 1L;
        schema = "shares";
        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setSubRequestId(UUID.randomUUID().toString());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql("create database shares");
        query = planner.parse(queryRequest.getSql());
        context = new DdlRequestContext(new DdlRequest(queryRequest));
        context.getRequest().setQueryRequest(queryRequest);
        context.setQuery(query);
    }

    @Test
    void executeSuccess() {
        Promise promise = Promise.promise();
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Boolean>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(false));
            return null;
        }).when(datamartDao).isDatamartExists(eq(schema), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(metadataExecutor).execute(any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(datamartId));
            return null;
        }).when(datamartDao).insertDatamart(eq(schema), any());

        createSchemaDdlExecutor.execute(context, null, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().result());
    }

    @Test
    void executeWithExistDatamart() {
        Promise promise = Promise.promise();
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Boolean>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(true));
            return null;
        }).when(datamartDao).isDatamartExists(eq(schema), any());

        createSchemaDdlExecutor.execute(context, null, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }

    @Test
    void executeWithCheckExistsDatamartError() {
        Promise promise = Promise.promise();
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Boolean>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(datamartDao).isDatamartExists(eq(schema), any());

        createSchemaDdlExecutor.execute(context, null, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }

    @Test
    void executeWithMetadataExecError() {
        Promise promise = Promise.promise();
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Boolean>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(false));
            return null;
        }).when(datamartDao).isDatamartExists(eq(schema), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(metadataExecutor).execute(any(), any());

        createSchemaDdlExecutor.execute(context, null, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }

    @Test
    void executeWithInsertDatamartError() {
        Promise promise = Promise.promise();
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Boolean>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(false));
            return null;
        }).when(datamartDao).isDatamartExists(eq(schema), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(metadataExecutor).execute(any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(datamartDao).insertDatamart(eq(schema), any());

        createSchemaDdlExecutor.execute(context, null, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }
}
