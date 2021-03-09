package io.arenadata.dtm.query.execution.core.service.ddl;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.cache.service.CaffeineCacheService;
import io.arenadata.dtm.cache.service.EvictQueryTemplateCacheService;
import io.arenadata.dtm.cache.service.EvictQueryTemplateCacheServiceImpl;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.framework.DtmCalciteFramework;
import io.arenadata.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.DatamartDaoImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.EntityDaoImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.ServiceDbDaoImpl;
import io.arenadata.dtm.query.execution.core.dto.cache.EntityKey;
import io.arenadata.dtm.query.execution.core.dto.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.core.exception.entity.EntityNotExistsException;
import io.arenadata.dtm.query.execution.core.service.datasource.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.datasource.impl.DataSourcePluginServiceImpl;
import io.arenadata.dtm.query.execution.core.service.ddl.impl.DropTableDdlExecutor;
import io.arenadata.dtm.query.execution.core.service.hsql.HSQLClient;
import io.arenadata.dtm.query.execution.core.service.hsql.impl.HSQLClientImpl;
import io.arenadata.dtm.query.execution.core.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.service.metadata.MetadataExecutorImpl;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.ResultSet;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class DropTableDdlExecutorTest {

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private final MetadataExecutor<DdlRequestContext> metadataExecutor = mock(MetadataExecutorImpl.class);
    private final DataSourcePluginService pluginService = mock(DataSourcePluginServiceImpl.class);
    private final CacheService<EntityKey, Entity> cacheService = mock(CaffeineCacheService.class);
    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDaoImpl.class);
    private final DatamartDao datamartDao = mock(DatamartDaoImpl.class);
    private final EntityDao entityDao = mock(EntityDaoImpl.class);
    private final EvictQueryTemplateCacheService evictQueryTemplateCacheService =
            mock(EvictQueryTemplateCacheServiceImpl.class);
    private final HSQLClient hsqlClient = mock(HSQLClientImpl.class);
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
        dropTableDdlExecutor = new DropTableDdlExecutor(cacheService, metadataExecutor, serviceDbFacade, pluginService, hsqlClient,
                evictQueryTemplateCacheService);
        doNothing().when(evictQueryTemplateCacheService).evictByEntityName(anyString(), anyString());

        schema = "shares";
        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);
        queryRequest.setSql("drop table accounts");
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);
        EntityField f1 = new EntityField(0, "id", ColumnType.INT, false);
        f1.setPrimaryOrder(1);
        f1.setShardingOrder(1);
        EntityField f2 = new EntityField(1, "name", ColumnType.VARCHAR, true);
        f2.setSize(100);
        String sqlNodeName = "accounts";
        Entity entity = new Entity(sqlNodeName, schema, Arrays.asList(f1, f2));
        entity.setEntityType(EntityType.TABLE);
        entity.setDestination(Collections.singleton(SourceType.ADB));
        context.setEntity(entity);
        context.setDatamartName(schema);
    }

    @Test
    void executeSuccess() {
        Promise<QueryResult> promise = Promise.promise();
        Entity entity = context.getEntity();
        when(pluginService.getSourceTypes()).thenReturn(new HashSet<>(Arrays.asList(SourceType.ADB)));

        when(entityDao.getEntity(eq(schema), eq(entity.getName())))
                .thenReturn(Future.succeededFuture(entity));

        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.EMPTY_LIST)));

        when(metadataExecutor.execute(any()))
                .thenReturn(Future.succeededFuture());

        when(entityDao.deleteEntity(eq(schema), eq(entity.getName())))
                .thenReturn(Future.succeededFuture());

        dropTableDdlExecutor.execute(context, context.getEntity().getName())
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
        verify(evictQueryTemplateCacheService, times(1))
                .evictByEntityName(entity.getSchema(), entity.getName());
    }

    @Test
    void executeWithFindView() {
        Promise<QueryResult> promise = Promise.promise();
        Entity entity = context.getEntity();
        when(pluginService.getSourceTypes()).thenReturn(new HashSet<>(Arrays.asList(SourceType.ADB)));

        when(entityDao.getEntity(eq(schema), eq(entity.getName())))
                .thenReturn(Future.succeededFuture(Entity.builder()
                        .schema(schema)
                        .name(entity.getName())
                        .entityType(EntityType.VIEW)
                        .build()));

        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.EMPTY_LIST)));

        dropTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().cause());
        verify(evictQueryTemplateCacheService, never())
                .evictByEntityName(entity.getSchema(), entity.getName());
    }

    @Test
    void executeWithIfExistsStmtSuccess() {
        Promise<QueryResult> promise = Promise.promise();
        Entity entity = context.getEntity();
        when(pluginService.getSourceTypes()).thenReturn(new HashSet<>(Arrays.asList(SourceType.ADB)));
        context.getRequest().getQueryRequest().setSql("DROP TABLE IF EXISTS accounts");
        String entityName = entity.getName();
        when(entityDao.getEntity(eq(schema), eq(entityName)))
                .thenReturn(Future.failedFuture(new EntityNotExistsException(entityName)));

        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.EMPTY_LIST)));

        dropTableDdlExecutor.execute(context, entityName)
                .onComplete(promise);
        assertNotNull(promise.future().result());
        verify(evictQueryTemplateCacheService, never())
                .evictByEntityName(entity.getSchema(), entity.getName());
    }

    @Test
    void executeWithMetadataExecuteError() {
        Promise<QueryResult> promise = Promise.promise();
        Entity entity = context.getEntity();
        when(pluginService.getSourceTypes()).thenReturn(new HashSet<>(Arrays.asList(SourceType.ADB)));
        when(entityDao.getEntity(eq(schema), eq(entity.getName())))
                .thenReturn(Future.succeededFuture(entity));

        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.EMPTY_LIST)));

        when(metadataExecutor.execute(any()))
                .thenReturn(Future.failedFuture(new DataSourceException("Error drop table in plugin")));

        dropTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().cause());
        verify(evictQueryTemplateCacheService, times(1))
                .evictByEntityName(entity.getSchema(), entity.getName());
    }

    @Test
    void executeWithDropEntityError() {
        Promise<QueryResult> promise = Promise.promise();
        Entity entity = context.getEntity();
        when(pluginService.getSourceTypes()).thenReturn(new HashSet<>(Arrays.asList(SourceType.ADB)));
        when(entityDao.getEntity(eq(schema), eq(entity.getName())))
                .thenReturn(Future.succeededFuture(context.getEntity()));

        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.EMPTY_LIST)));

        when(metadataExecutor.execute(any()))
                .thenReturn(Future.succeededFuture());

        when(entityDao.deleteEntity(eq(schema), eq(entity.getName())))
                .thenReturn(Future.failedFuture(new DtmException("delete entity error")));

        dropTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().cause());
        verify(evictQueryTemplateCacheService, times(1))
                .evictByEntityName(entity.getSchema(), entity.getName());
    }

    @Test
    void executeWithExistedViewError() {
        Promise<QueryResult> promise = Promise.promise();
        String viewName = "VIEWNAME";
        String expectedMessage = String.format("View ‘%s’ using the '%s' must be dropped first", viewName, context.getEntity().getName().toUpperCase());

        when(pluginService.getSourceTypes()).thenReturn(new HashSet<>(Arrays.asList(SourceType.ADB)));

        when(entityDao.getEntity(eq(schema), eq(context.getEntity().getName())))
                .thenReturn(Future.succeededFuture(context.getEntity()));

        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.singletonList(new JsonArray().add(viewName)))));

        dropTableDdlExecutor.execute(context, context.getEntity().getName())
                .onComplete(promise);
        assertEquals(expectedMessage, promise.future().cause().getMessage());
    }
}
