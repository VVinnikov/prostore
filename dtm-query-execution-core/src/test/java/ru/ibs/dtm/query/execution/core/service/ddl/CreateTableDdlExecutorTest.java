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
import ru.ibs.dtm.query.execution.core.dao.servicedb.*;
import ru.ibs.dtm.query.execution.core.dao.servicedb.impl.*;
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

    private CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private Planner planner;
    private final MetadataCalciteGenerator metadataCalciteGenerator = mock(MetadataCalciteGeneratorImpl.class);
    private final MetadataExecutor<DdlRequestContext> metadataExecutor = mock(MetadataExecutorImpl.class);
    private final MariaProperties mariaProperties = mock(MariaProperties.class);
    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDaoImpl.class);
    private final EntityDao entityDao = mock(EntityDaoImpl.class);
    private final DatamartDao datamartDao = mock(DatamartDaoImpl.class);
    private final AttributeDao attributeDao = mock(AttributeDaoImpl.class);
    private final AttributeTypeDao attributeTypeDao = mock(AttributeTypeDaoImpl.class);
    private QueryResultDdlExecutor createTableDdlExecutor;
    private DdlRequestContext context;
    private SqlNode query;
    private ClassTable classTable;
    private Long datamartId;
    private String schema;

    @BeforeEach
    void setUp() throws SqlParseException {
        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        planner = Frameworks.getPlanner(frameworkConfig);
        createTableDdlExecutor = new CreateTableDdlExecutor(metadataExecutor,
                mariaProperties, serviceDbFacade, metadataCalciteGenerator);
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        when(serviceDbDao.getAttributeDao()).thenReturn(attributeDao);
        when(serviceDbDao.getAttributeTypeDao()).thenReturn(attributeTypeDao);

        schema = "shares";
        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setSubRequestId(UUID.randomUUID().toString());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql("create table accounts (id integer, name varchar(100))");
        query = planner.parse(queryRequest.getSql());
        context = new DdlRequestContext(new DdlRequest(queryRequest));
        context.getRequest().setQueryRequest(queryRequest);
        context.setQuery(query);
        datamartId = 1L;
        ClassField f1 = new ClassField(0,"id", ClassTypes.INT, false, true);
        ClassField f2 = new ClassField(1, "name", ClassTypes.VARCHAR, true, false);
        f2.setSize(100);
        String sqlNodeName = "accounts";
        classTable = new ClassTable(sqlNodeName, schema, Arrays.asList(f1, f2));
    }

    @Test
    void executeSuccess() {
        Promise promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(classTable);

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(datamartId));
            return null;
        }).when(datamartDao).findDatamart(eq(schema), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Boolean>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture(false));
            return null;
        }).when(entityDao).isEntityExists(eq(datamartId), eq(classTable.getName()), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(metadataExecutor).execute(any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(entityDao).insertEntity(eq(datamartId), eq(classTable.getName()), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture(1L));
            return null;
        }).when(entityDao).findEntity(eq(datamartId), eq(classTable.getName()), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Integer>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(1));
            return null;
        }).when(attributeTypeDao).findTypeIdByTypeMnemonic(any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Integer>> handler = invocation.getArgument(3);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(attributeDao).insertAttribute(any(), any(), any(), any());

        createTableDdlExecutor.execute(context, classTable.getName(), ar -> {
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

        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(classTable);

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(datamartDao).findDatamart(any(), any());

        createTableDdlExecutor.execute(context, classTable.getName(), ar -> {
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
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(classTable);

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(datamartId));
            return null;
        }).when(datamartDao).findDatamart(eq(schema), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Boolean>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture(true));
            return null;
        }).when(entityDao).isEntityExists(eq(datamartId), eq(classTable.getName()), any());

        createTableDdlExecutor.execute(context, classTable.getName(), ar -> {
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
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(classTable);

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(datamartId));
            return null;
        }).when(datamartDao).findDatamart(eq(schema), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Boolean>> handler = invocation.getArgument(2);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(entityDao).isEntityExists(eq(datamartId), eq(classTable.getName()), any());

        createTableDdlExecutor.execute(context, classTable.getName(), ar -> {
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
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(classTable);

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(datamartId));
            return null;
        }).when(datamartDao).findDatamart(eq(schema), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Boolean>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture(false));
            return null;
        }).when(entityDao).isEntityExists(eq(datamartId), eq(classTable.getName()), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(metadataExecutor).execute(any(), any());

        createTableDdlExecutor.execute(context, classTable.getName(), ar -> {
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
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(classTable);

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(datamartId));
            return null;
        }).when(datamartDao).findDatamart(eq(schema), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Boolean>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture(false));
            return null;
        }).when(entityDao).isEntityExists(eq(datamartId), eq(classTable.getName()), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(metadataExecutor).execute(any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(2);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(entityDao).insertEntity(eq(datamartId), eq(classTable.getName()), any());

        createTableDdlExecutor.execute(context, classTable.getName(), ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }

    @Test
    void executeWithCreateAttributesError() {
        Promise promise = Promise.promise();
        when(metadataCalciteGenerator.generateTableMetadata(any())).thenReturn(classTable);

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(datamartId));
            return null;
        }).when(datamartDao).findDatamart(eq(schema), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Boolean>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture(false));
            return null;
        }).when(entityDao).isEntityExists(eq(datamartId), eq(classTable.getName()), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(metadataExecutor).execute(any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(entityDao).insertEntity(eq(datamartId), eq(classTable.getName()), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Long>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture(1L));
            return null;
        }).when(entityDao).findEntity(eq(datamartId), eq(classTable.getName()), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Integer>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(1));
            return null;
        }).when(attributeTypeDao).findTypeIdByTypeMnemonic(any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(3);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(attributeDao).insertAttribute(any(), any(), any(), any());

        createTableDdlExecutor.execute(context, classTable.getName(), ar -> {
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

        createTableDdlExecutor.execute(context, classTable.getName(), ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }
}
