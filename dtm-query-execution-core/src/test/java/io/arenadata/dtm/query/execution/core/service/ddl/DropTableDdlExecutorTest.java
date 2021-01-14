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
import io.arenadata.dtm.query.execution.core.exception.table.TableNotExistsException;
import io.arenadata.dtm.query.execution.core.service.cache.EntityCacheService;
import io.arenadata.dtm.query.execution.core.service.datasource.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.dto.cache.EntityKey;
import io.arenadata.dtm.query.execution.core.service.datasource.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.datasource.impl.DataSourcePluginServiceImpl;
import io.arenadata.dtm.query.execution.core.service.ddl.impl.DropTableDdlExecutor;
import io.arenadata.dtm.query.execution.core.service.hsql.HSQLClient;
import io.arenadata.dtm.query.execution.core.service.hsql.impl.HSQLClientImpl;
import io.arenadata.dtm.query.execution.core.service.ddl.impl.DropTableDdlExecutor;
import io.arenadata.dtm.query.execution.core.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.service.metadata.impl.MetadataExecutorImpl;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
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
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.doNothing;

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
        dropTableDdlExecutor = new DropTableDdlExecutor(cacheService, metadataExecutor, serviceDbFacade, pluginService,
                evictQueryTemplateCacheService);
        doNothing().when(evictQueryTemplateCacheService).evictByEntityName(anyString(), anyString());
        dropTableDdlExecutor = new DropTableDdlExecutor(cacheService, metadataExecutor, serviceDbFacade, pluginService, hsqlClient);

        schema = "shares";
        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
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
        entity.setDestination(Collections.singleton(SourceType.ADB));
        context.getRequest().setEntity(entity);
    }

    @Test
    void executeSuccess() {
        Promise<QueryResult> promise = Promise.promise();
        Entity entity = context.getRequest().getEntity();
        when(pluginService.getSourceTypes()).thenReturn(new HashSet<>(Arrays.asList(SourceType.ADB)));

        when(entityDao.getEntity(eq(schema), eq(entity.getName())))
                .thenReturn(Future.succeededFuture(entity));

        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.EMPTY_LIST)));

        when(metadataExecutor.execute(any()))
                .thenReturn(Future.succeededFuture());

        when(entityDao.deleteEntity(eq(schema), eq(entity.getName())))
                .thenReturn(Future.succeededFuture());

        dropTableDdlExecutor.execute(context, entity.getName())

        dropTableDdlExecutor.execute(context, context.getRequest().getEntity().getName())
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
        verify(evictQueryTemplateCacheService, times(1))
                .evictByEntityName(entity.getSchema(), entity.getName());
    }

    @Test
    void executeWithFindView() {
        Promise<QueryResult> promise = Promise.promise();
        Entity entity = context.getRequest().getEntity();
        when(pluginService.getSourceTypes()).thenReturn(new HashSet<>(Arrays.asList(SourceType.ADB)));

        when(entityDao.getEntity(eq(schema), eq(entity.getName())))
                .thenReturn(Future.succeededFuture(Entity.builder()
                        .schema(schema)
                        .name(entity.getName())
                        .entityType(EntityType.VIEW)
                        .build()));

        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.EMPTY_LIST)));

        dropTableDdlExecutor.execute(context, context.getRequest().getEntity().getName())
        dropTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().cause());
        verify(evictQueryTemplateCacheService, never())
                .evictByEntityName(entity.getSchema(), entity.getName());
    }

    @Test
    void executeWithIfExistsStmtSuccess() {
        Promise<QueryResult> promise = Promise.promise();
        Entity entity = context.getRequest().getEntity();
        when(pluginService.getSourceTypes()).thenReturn(new HashSet<>(Arrays.asList(SourceType.ADB)));
        context.getRequest().getQueryRequest().setSql("DROP TABLE IF EXISTS accounts");
        String entityName = entity.getName();
        Mockito.when(entityDao.getEntity(eq(schema), eq(entityName)))
                .thenReturn(Future.failedFuture(new TableNotExistsException(entityName)));

        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.EMPTY_LIST)));

        dropTableDdlExecutor.execute(context, entityName)
                .onComplete(promise);
        assertNotNull(promise.future().result());
        verify(evictQueryTemplateCacheService, times(1))
                .evictByEntityName(entity.getSchema(), entity.getName());
    }

    @Test
    void executeWithMetadataExecuteError() {
        Promise<QueryResult> promise = Promise.promise();
        Entity entity = context.getRequest().getEntity();
        when(pluginService.getSourceTypes()).thenReturn(new HashSet<>(Arrays.asList(SourceType.ADB)));
        Mockito.when(entityDao.getEntity(eq(schema), eq(entity.getName())))
                .thenReturn(Future.succeededFuture(entity));

        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.EMPTY_LIST)));

        when(metadataExecutor.execute(any()))
                .thenReturn(Future.failedFuture(new DataSourceException("Error drop table in plugin")));

        dropTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().cause());
        verify(evictQueryTemplateCacheService, never())
                .evictByEntityName(entity.getSchema(), entity.getName());
    }

    @Test
    void executeWithDropEntityError() {
        Promise<QueryResult> promise = Promise.promise();
        Entity entity = context.getRequest().getEntity();
        when(pluginService.getSourceTypes()).thenReturn(new HashSet<>(Arrays.asList(SourceType.ADB)));
        Mockito.when(entityDao.getEntity(eq(schema), eq(entity.getName())))
                .thenReturn(Future.succeededFuture(context.getRequest().getEntity()));

        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.EMPTY_LIST)));

        when(metadataExecutor.execute(any()))
                .thenReturn(Future.succeededFuture());

        Mockito.when(entityDao.deleteEntity(eq(schema), eq(entity.getName())))
                .thenReturn(Future.failedFuture(new DtmException("delete entity error")));

        dropTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);
        assertNotNull(promise.future().cause());
        verify(evictQueryTemplateCacheService, never())
                .evictByEntityName(entity.getSchema(), entity.getName());
    }

    @Test
    void executeWithExistedViewError() {
        Promise<QueryResult> promise = Promise.promise();
        String viewName = "VIEWNAME";
        String expectedMessage = String.format("View ‘%s’ using the '%s' must be dropped first", viewName, context.getRequest().getEntity().getName().toUpperCase());

        when(pluginService.getSourceTypes()).thenReturn(new HashSet<>(Arrays.asList(SourceType.ADB)));

        when(entityDao.getEntity(eq(schema), eq(context.getRequest().getEntity().getName())))
                .thenReturn(Future.succeededFuture(context.getRequest().getEntity()));

        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.singletonList(new JsonArray().add(viewName)))));

        dropTableDdlExecutor.execute(context, context.getRequest().getEntity().getName())
                .onComplete(promise);
        assertEquals(expectedMessage, promise.future().cause().getMessage());
    }
}
