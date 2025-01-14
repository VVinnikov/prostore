/*
 * Copyright © 2021 ProStore
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.arenadata.dtm.query.execution.core.ddl;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.cache.service.EvictQueryTemplateCacheService;
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
import io.arenadata.dtm.query.execution.core.base.dto.cache.EntityKey;
import io.arenadata.dtm.query.execution.core.base.exception.entity.EntityNotExistsException;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.base.service.hsql.HSQLClient;
import io.arenadata.dtm.query.execution.core.base.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
import io.arenadata.dtm.query.execution.core.ddl.service.QueryResultDdlExecutor;
import io.arenadata.dtm.query.execution.core.ddl.service.impl.DropTableExecutor;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
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
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DropTableExecutorTest {
    private static final String SCHEMA = "shares";

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
    private final SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private final FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();

    @Mock
    private MetadataExecutor<DdlRequestContext> metadataExecutor;
    @Mock
    private DataSourcePluginService pluginService;
    @Mock
    private CacheService<EntityKey, Entity> cacheService;
    @Mock
    private ServiceDbFacade serviceDbFacade;
    @Mock
    private ServiceDbDao serviceDbDao;
    @Mock
    private DatamartDao datamartDao;
    @Mock
    private EntityDao entityDao;
    @Mock
    private EvictQueryTemplateCacheService evictQueryTemplateCacheService;
    @Mock
    private HSQLClient hsqlClient;

    @Captor
    private ArgumentCaptor<DdlRequestContext> contextArgumentCaptor;

    private QueryResultDdlExecutor dropTableDdlExecutor;
    private DdlRequestContext context;

    @BeforeEach
    void setUp() {
        lenient().when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        lenient().when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        lenient().when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        dropTableDdlExecutor = new DropTableExecutor(cacheService, metadataExecutor, serviceDbFacade, pluginService, hsqlClient,
                evictQueryTemplateCacheService);
        lenient().doNothing().when(evictQueryTemplateCacheService).evictByEntityName(anyString(), anyString());
    }

    @Test
    void executeSuccess() throws SqlParseException {
        // arrange
        prepareContext("drop table accounts");
        Promise<QueryResult> promise = Promise.promise();
        Entity entity = context.getEntity();
        when(pluginService.getSourceTypes()).thenReturn(Collections.singleton(SourceType.ADB));

        when(entityDao.getEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture(entity));
        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.EMPTY_LIST)));
        when(metadataExecutor.execute(any()))
                .thenReturn(Future.succeededFuture());
        when(entityDao.deleteEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture());

        // act
        dropTableDdlExecutor.execute(context, context.getEntity().getName())
                .onComplete(promise);

        // assert
        assertTrue(promise.future().succeeded());
        verify(evictQueryTemplateCacheService)
                .evictByEntityName(entity.getSchema(), entity.getName());
        verify(metadataExecutor).execute(contextArgumentCaptor.capture());
        DdlRequestContext value = contextArgumentCaptor.getValue();
        assertNull(value.getSourceType());
    }

    @Test
    void executeSuccessLogicalOnly() throws SqlParseException {
        // arrange
        prepareContext("drop table accounts logical_only");
        Promise<QueryResult> promise = Promise.promise();
        Entity entity = context.getEntity();
        when(pluginService.getSourceTypes()).thenReturn(Collections.singleton(SourceType.ADB));

        when(entityDao.getEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture(entity));
        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.EMPTY_LIST)));
        when(entityDao.deleteEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture());

        // act
        dropTableDdlExecutor.execute(context, context.getEntity().getName())
                .onComplete(promise);

        // assert
        assertTrue(promise.future().succeeded());
        verify(evictQueryTemplateCacheService)
                .evictByEntityName(entity.getSchema(), entity.getName());
        verify(metadataExecutor, never()).execute(any());
    }

    @Test
    void executeWithIfExistsStmtSuccess() throws SqlParseException {
        // arrange
        prepareContext("drop table accounts");
        Promise<QueryResult> promise = Promise.promise();
        Entity entity = context.getEntity();
        context.getRequest().getQueryRequest().setSql("DROP TABLE IF EXISTS accounts");
        String entityName = entity.getName();
        when(entityDao.getEntity(SCHEMA, entityName))
                .thenReturn(Future.failedFuture(new EntityNotExistsException(entityName)));

        // act
        dropTableDdlExecutor.execute(context, entityName)
                .onComplete(promise);

        // assert
        assertNotNull(promise.future().result());
        verify(evictQueryTemplateCacheService, never())
                .evictByEntityName(entity.getSchema(), entity.getName());
        verifyNoInteractions(pluginService, hsqlClient);
    }

    @Test
    void executeWithMetadataExecuteError() throws SqlParseException {
        // arrange
        prepareContext("drop table accounts");
        Promise<QueryResult> promise = Promise.promise();
        Entity entity = context.getEntity();
        when(pluginService.getSourceTypes()).thenReturn(Collections.singleton(SourceType.ADB));
        when(entityDao.getEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture(entity));

        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.EMPTY_LIST)));

        when(metadataExecutor.execute(any()))
                .thenReturn(Future.failedFuture(new DataSourceException("Error drop table in plugin")));

        // act
        dropTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);

        // assert
        assertNotNull(promise.future().cause());
        verify(evictQueryTemplateCacheService, times(1))
                .evictByEntityName(entity.getSchema(), entity.getName());
    }

    @Test
    void executeWithDropEntityError() throws SqlParseException {
        // arrange
        prepareContext("drop table accounts");
        Promise<QueryResult> promise = Promise.promise();
        Entity entity = context.getEntity();
        when(pluginService.getSourceTypes()).thenReturn(Collections.singleton(SourceType.ADB));
        when(entityDao.getEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture(context.getEntity()));

        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.EMPTY_LIST)));

        when(metadataExecutor.execute(any()))
                .thenReturn(Future.succeededFuture());

        when(entityDao.deleteEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.failedFuture(new DtmException("delete entity error")));

        // act
        dropTableDdlExecutor.execute(context, entity.getName())
                .onComplete(promise);

        // assert
        assertNotNull(promise.future().cause());
        verify(evictQueryTemplateCacheService)
                .evictByEntityName(entity.getSchema(), entity.getName());
    }

    @Test
    void executeWithExistedViewError() throws SqlParseException {
        // arrange
        prepareContext("drop table accounts");
        Promise<QueryResult> promise = Promise.promise();
        String viewName1 = "SYS_VIEWNAME_1";
        String viewName2 = "VIEWNAME_2";
        JsonArray views = new JsonArray().add(viewName1).add(viewName2);
        String expectedMessage = String.format("Views [VIEWNAME_1, VIEWNAME_2] using the '%s' must be dropped first", context.getEntity().getName().toUpperCase());

        when(entityDao.getEntity(SCHEMA, context.getEntity().getName()))
                .thenReturn(Future.succeededFuture(context.getEntity()));

        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.singletonList(views))));

        // act
        dropTableDdlExecutor.execute(context, context.getEntity().getName())
                .onComplete(promise);

        // assert
        assertEquals(expectedMessage, promise.future().cause().getMessage());
    }

    @Test
    void executeCorrectlyExtractSourceType() throws SqlParseException {
        // arrange
        prepareContext("drop table accounts datasource_type = 'ADB'");
        Entity entity = context.getEntity();

        Promise<QueryResult> promise = Promise.promise();

        when(pluginService.getSourceTypes()).thenReturn(Collections.singleton(SourceType.ADB));

        when(entityDao.getEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture(entity));

        when(hsqlClient.getQueryResult(any()))
                .thenReturn(Future.succeededFuture(new ResultSet().setResults(Collections.EMPTY_LIST)));

        when(metadataExecutor.execute(any()))
                .thenReturn(Future.succeededFuture());

        when(entityDao.deleteEntity(SCHEMA, entity.getName()))
                .thenReturn(Future.succeededFuture());

        // act
        dropTableDdlExecutor.execute(context, context.getEntity().getName())
                .onComplete(promise);

        // assert
        assertTrue(promise.future().succeeded());
        verify(metadataExecutor).execute(contextArgumentCaptor.capture());
        DdlRequestContext value = contextArgumentCaptor.getValue();
        assertSame(SourceType.ADB, value.getSourceType());
    }

    private void prepareContext(String s) throws SqlParseException {
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(SCHEMA);
        queryRequest.setSql(s);
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);
        EntityField f1 = new EntityField(0, "id", ColumnType.INT, false);
        f1.setPrimaryOrder(1);
        f1.setShardingOrder(1);
        EntityField f2 = new EntityField(1, "name", ColumnType.VARCHAR, true);
        f2.setSize(100);
        String sqlNodeName = "accounts";
        Entity ctxEntity = new Entity(sqlNodeName, SCHEMA, Arrays.asList(f1, f2));
        ctxEntity.setEntityType(EntityType.TABLE);
        ctxEntity.setDestination(Collections.singleton(SourceType.ADB));
        context.setEntity(ctxEntity);
        context.setDatamartName(SCHEMA);
    }
}
