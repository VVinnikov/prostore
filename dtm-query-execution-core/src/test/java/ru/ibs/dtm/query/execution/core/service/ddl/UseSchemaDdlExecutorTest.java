package ru.ibs.dtm.query.execution.core.service.ddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.ibs.dtm.common.model.ddl.ColumnType;
import ru.ibs.dtm.common.model.ddl.SystemMetadata;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
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
import ru.ibs.dtm.query.execution.core.service.ddl.impl.UseSchemaDdlExecutor;
import ru.ibs.dtm.query.execution.core.service.metadata.MetadataExecutor;
import ru.ibs.dtm.query.execution.core.service.metadata.impl.MetadataExecutorImpl;
import ru.ibs.dtm.query.execution.core.utils.QueryResultUtils;
import ru.ibs.dtm.query.execution.model.metadata.ColumnMetadata;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DdlRequest;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class UseSchemaDdlExecutorTest {

    private CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private Planner planner;
    private final MetadataExecutor<DdlRequestContext> metadataExecutor = mock(MetadataExecutorImpl.class);
    private final MariaProperties mariaProperties = mock(MariaProperties.class);
    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDaoImpl.class);
    private final DatamartDao datamartDao = mock(DatamartDaoImpl.class);
    private QueryResultDdlExecutor useSchemaDdlExecutor;
    private DdlRequestContext context;
    private SqlNode query;
    private String schema;
    private Long datamartId;

    @BeforeEach
    void setUp() throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        datamartId = 1L;
        schema = "shares";
        useSchemaDdlExecutor = new UseSchemaDdlExecutor(metadataExecutor, mariaProperties, serviceDbFacade);
        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setSubRequestId(UUID.randomUUID().toString());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql("USE shares");
        query = planner.parse(queryRequest.getSql());
        context = new DdlRequestContext(new DdlRequest(queryRequest));
        context.getRequest().setQueryRequest(queryRequest);
        context.setQuery(query);
    }

    @Test
    void executeSuccess() {
        Promise promise = Promise.promise();
        QueryResult result = new QueryResult();
        result.setMetadata(Collections.singletonList(new ColumnMetadata("schema", SystemMetadata.SCHEMA, ColumnType.VARCHAR)));
        result.setRequestId(context.getRequest().getQueryRequest().getRequestId());
        result.setResult(QueryResultUtils.createResultWithSingleRow(Collections.singletonList("schema"),
                Collections.singletonList(schema)));
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(datamartId));
            return null;
        }).when(datamartDao).findDatamart(eq(schema), any());

        useSchemaDdlExecutor.execute(context, schema, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().result());
        assertEquals(result, promise.future().result());
    }

    @Test
    void executeDatamartIsNotExists() {
        Promise promise = Promise.promise();
        QueryResult result = new QueryResult();
        result.setMetadata(Collections.singletonList(new ColumnMetadata("schema", SystemMetadata.SCHEMA, ColumnType.VARCHAR)));
        result.setRequestId(context.getRequest().getQueryRequest().getRequestId());
        result.setResult(QueryResultUtils.createResultWithSingleRow(Collections.singletonList("schema"),
                Collections.singletonList(schema)));
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(datamartDao).findDatamart(eq(schema), any());

        useSchemaDdlExecutor.execute(context, schema, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().failed());
    }

    @Test
    void executeIncorrectQuery() {
        Promise promise = Promise.promise();
        QueryResult result = new QueryResult();
        result.setMetadata(Collections.singletonList(new ColumnMetadata("schema",
                SystemMetadata.SCHEMA, ColumnType.VARCHAR)));
        result.setRequestId(context.getRequest().getQueryRequest().getRequestId());
        result.setResult(QueryResultUtils.createResultWithSingleRow(Collections.singletonList("schema"),
                Collections.singletonList(schema)));
        context.getRequest().getQueryRequest().setSql("USE_dtm");
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(datamartDao).findDatamart(eq(schema), any());

        useSchemaDdlExecutor.execute(context, schema, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().failed());
    }
}
