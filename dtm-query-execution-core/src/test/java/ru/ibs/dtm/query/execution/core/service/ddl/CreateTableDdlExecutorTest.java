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
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.ibs.dtm.query.calcite.core.framework.DtmCalciteFramework;
import ru.ibs.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import ru.ibs.dtm.query.execution.core.dao.exception.datamart.DatamartNotExistsException;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.ServiceDbDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.impl.DatamartDaoImpl;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.impl.EntityDaoImpl;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.impl.ServiceDbDaoImpl;
import ru.ibs.dtm.query.execution.core.service.ddl.impl.CreateTableDdlExecutor;
import ru.ibs.dtm.query.execution.core.service.metadata.MetadataCalciteGenerator;
import ru.ibs.dtm.query.execution.core.service.metadata.MetadataExecutor;
import ru.ibs.dtm.query.execution.core.service.metadata.impl.MetadataCalciteGeneratorImpl;
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

class CreateTableDdlExecutorTest {

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private final MetadataCalciteGenerator metadataCalciteGenerator = mock(MetadataCalciteGeneratorImpl.class);
    private final MetadataExecutor<DdlRequestContext> metadataExecutor = mock(MetadataExecutorImpl.class);
    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDaoImpl.class);
    private final DatamartDao datamartDao = mock(DatamartDaoImpl.class);
    private final EntityDao entityDao = mock(EntityDaoImpl.class);
    private QueryResultDdlExecutor createTableDdlExecutor;
    private DdlRequestContext context;
    private Entity entity;
    private String schema;

    @BeforeEach
    void setUp() throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        createTableDdlExecutor = new CreateTableDdlExecutor(metadataExecutor, serviceDbFacade, metadataCalciteGenerator);

        schema = "shares";
        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setSubRequestId(UUID.randomUUID().toString());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql("create table accounts (id integer, name varchar(100))");
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
        entity = new Entity(sqlNodeName, schema, Arrays.asList(f1, f2));
    }

    @Test
    void executeSuccess() {
        Promise promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
            .thenReturn(Future.succeededFuture(true));

        Mockito.when(entityDao.existsEntity(eq(schema), eq(entity.getName())))
            .thenReturn(Future.succeededFuture(false));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(metadataExecutor).execute(any(), any());

        Mockito.when(entityDao.createEntity(any()))
            .thenReturn(Future.succeededFuture());

        createTableDdlExecutor.execute(context, entity.getName(), ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().result());
    }

    @Test
    void executeWithExistsDatamartError() {
        Promise promise = Promise.promise();

        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
            .thenReturn(Future.failedFuture(new DatamartNotExistsException(schema)));

        createTableDdlExecutor.execute(context, entity.getName(), ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }

    @Test
    void executeWithNotExistsDatamart() {
        Promise promise = Promise.promise();

        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
            .thenReturn(Future.succeededFuture(false));

        createTableDdlExecutor.execute(context, entity.getName(), ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }

    @Test
    void executeWithTableExists() {
        Promise promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);


        Mockito.when(datamartDao.existsDatamart(eq(schema)))
            .thenReturn(Future.succeededFuture(true));

        Mockito.when(entityDao.existsEntity(eq(schema), eq(entity.getName())))
            .thenReturn(Future.succeededFuture(true));

        createTableDdlExecutor.execute(context, entity.getName(), ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }

    @Test
    void executeWithTableExistsError() {
        Promise promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
            .thenReturn(Future.succeededFuture(true));

        Mockito.when(entityDao.existsEntity(eq(schema), eq(entity.getName())))
            .thenReturn(Future.failedFuture("exists entity error"));

        createTableDdlExecutor.execute(context, entity.getName(), ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }

    @Test
    void executeWithMetadataDataSourceError() {
        Promise promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
            .thenReturn(Future.succeededFuture(true));

        Mockito.when(entityDao.existsEntity(eq(schema), eq(entity.getName())))
            .thenReturn(Future.succeededFuture(false));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(metadataExecutor).execute(any(), any());

        createTableDdlExecutor.execute(context, entity.getName(), ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }

    @Test
    void executeWithInsertTableError() {
        Promise promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(entity);


        Mockito.when(datamartDao.existsDatamart(eq(schema)))
            .thenReturn(Future.succeededFuture(true));

        Mockito.when(entityDao.existsEntity(eq(schema), eq(entity.getName())))
            .thenReturn(Future.succeededFuture(false));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(metadataExecutor).execute(any(), any());

        Mockito.when(entityDao.createEntity(any()))
            .thenReturn(Future.failedFuture("create entity error"));

        createTableDdlExecutor.execute(context, entity.getName(), ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }

    @Test
    void executeWithMetadataGeneratorError() {
        Promise promise = Promise.promise();

        when(metadataCalciteGenerator.generateTableMetadata(any())).thenThrow(new RuntimeException());

        createTableDdlExecutor.execute(context, entity.getName(), ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }
}
