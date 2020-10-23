package io.arenadata.dtm.query.execution.core.service.ddl;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.framework.DtmCalciteFramework;
import io.arenadata.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.dao.exception.entity.ViewNotExistsException;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.DatamartDaoImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.EntityDaoImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.ServiceDbDaoImpl;
import io.arenadata.dtm.query.execution.core.service.ddl.impl.AlterViewDdlExecutor;
import io.arenadata.dtm.query.execution.core.service.dml.ColumnMetadataService;
import io.arenadata.dtm.query.execution.core.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.service.metadata.impl.MetadataExecutorImpl;
import io.arenadata.dtm.query.execution.core.service.schema.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.core.service.schema.impl.LogicalSchemaProviderImpl;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AlterViewDdlExecutorTest {

    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDaoImpl.class);
    private final EntityDao entityDao = mock(EntityDaoImpl.class);
    private final DatamartDao datamartDao = mock(DatamartDaoImpl.class);
    private final LogicalSchemaProvider logicalSchemaProvider = mock(LogicalSchemaProviderImpl.class);
    private final ColumnMetadataService columnMetadataService = mock(ColumnMetadataService.class);
    private final MetadataExecutor<DdlRequestContext> metadataExecutor = mock(MetadataExecutorImpl.class);
    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration
        .configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private AlterViewDdlExecutor alterViewDdlExecutor;
    private DdlRequestContext context;
    private String sqlNodeName;
    private String viewName;
    private String schema;

    @BeforeEach
    void setUp() throws SqlParseException {
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);

        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        alterViewDdlExecutor = new AlterViewDdlExecutor(metadataExecutor,
            logicalSchemaProvider,
            columnMetadataService,
            serviceDbFacade,
            new SqlDialect(SqlDialect.EMPTY_CONTEXT));
        schema = "shares";
        viewName = "test_view";
        sqlNodeName = schema + "." + viewName;
        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql(String.format("ALTER VIEW %s.test_view AS SELECT * FROM %s.test_table",
            schema, schema));
        SqlNode query = planner.parse(queryRequest.getSql());
        context = new DdlRequestContext(new DdlRequest(queryRequest));
        context.getRequest().setQueryRequest(queryRequest);
        context.setQuery(query);
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(metadataExecutor).execute(any(), any());
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<Datamart>>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(
                Collections.singletonList(new Datamart(
                    schema,
                    true,
                    Collections.singletonList(getViewEntity(schema, viewName))
                ))
            ));
            return null;
        }).when(logicalSchemaProvider).getSchema(any(), any());
    }

    @Test
    void executeSuccess() {
        Promise promise = Promise.promise();
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<ColumnMetadata>>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(Collections.singletonList(ColumnMetadata.builder()
                .name("id")
                .type(ColumnType.BIGINT)
                .build())));
            return null;
        }).when(columnMetadataService).getColumnMetadata(any(), any());
        Mockito.when(entityDao.getEntity(eq(schema), eq(viewName)))
            .thenReturn(Future.succeededFuture(getViewEntity(schema, viewName)));
        Mockito.when(entityDao.updateEntity(eq(getViewEntity(schema, viewName))))
            .thenReturn(Future.succeededFuture());
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

    private Entity getViewEntity(String schema, String viewName) {
        return Entity.builder()
            .entityType(EntityType.VIEW)
            .viewQuery("SELECT *\n" +
                "FROM shares.test_table")
            .name(viewName)
            .schema(schema)
            .fields(Collections.singletonList(EntityField.builder()
                .name("id")
                .type(ColumnType.BIGINT)
                .ordinalPosition(0)
                .nullable(true)
                .build()))
            .build();
    }

    private Entity getTableEntity(String schema, String viewName) {
        return Entity.builder()
            .entityType(EntityType.TABLE)
            .viewQuery("SELECT *\n" +
                "FROM shares.test_table")
            .name(viewName)
            .schema(schema)
            .fields(Collections.singletonList(EntityField.builder()
                .name("id")
                .type(ColumnType.BIGINT)
                .ordinalPosition(0)
                .nullable(true)
                .build()))
            .build();
    }

    @Test
    void executeIsEntityExistsError() {
        Promise promise = Promise.promise();
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<ColumnMetadata>>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(Collections.singletonList(ColumnMetadata.builder()
                .name("id")
                .type(ColumnType.BIGINT)
                .build())));
            return null;
        }).when(columnMetadataService).getColumnMetadata(any(), any());
        Mockito.when(entityDao.getEntity(eq(schema), eq(viewName)))
            .thenReturn(Future.failedFuture(new ViewNotExistsException(viewName)));
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
            final Handler<AsyncResult<List<ColumnMetadata>>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(Collections.singletonList(ColumnMetadata.builder()
                .name("id")
                .type(ColumnType.BIGINT)
                .build())));
            return null;
        }).when(columnMetadataService).getColumnMetadata(any(), any());
        Mockito.when(entityDao.getEntity(eq(schema), eq(viewName)))
            .thenReturn(Future.succeededFuture(getViewEntity(schema, viewName)));
        Mockito.when(entityDao.updateEntity(eq(getViewEntity(schema, viewName))))
            .thenReturn(Future.failedFuture("Update error"));
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
