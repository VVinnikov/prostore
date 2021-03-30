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
package io.arenadata.dtm.query.execution.core.service.ddl;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.cache.service.CaffeineCacheService;
import io.arenadata.dtm.cache.service.EvictQueryTemplateCacheService;
import io.arenadata.dtm.cache.service.EvictQueryTemplateCacheServiceImpl;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.InformationSchemaView;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.framework.DtmCalciteFramework;
import io.arenadata.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.DatamartDaoImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.ServiceDbDaoImpl;
import io.arenadata.dtm.query.execution.core.dto.cache.EntityKey;
import io.arenadata.dtm.query.execution.core.dto.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.core.dto.delta.HotDelta;
import io.arenadata.dtm.query.execution.core.dto.delta.OkDelta;
import io.arenadata.dtm.query.execution.core.service.ddl.impl.DropSchemaDdlExecutor;
import io.arenadata.dtm.query.execution.core.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.service.metadata.MetadataExecutorImpl;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class DropSchemaDdlExecutorTest {

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private final MetadataExecutor<DdlRequestContext> metadataExecutor = mock(MetadataExecutorImpl.class);
    private final CacheService<String, HotDelta> hotDeltaCacheService = mock(CaffeineCacheService.class);
    private final CacheService<String, OkDelta> okDeltaCacheService = mock(CaffeineCacheService.class);
    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDaoImpl.class);
    private final DatamartDao datamartDao = mock(DatamartDaoImpl.class);
    private final CacheService<EntityKey, Entity> entityCacheService = mock(CaffeineCacheService.class);
    private final EvictQueryTemplateCacheService evictQueryTemplateCacheService =
            mock(EvictQueryTemplateCacheServiceImpl.class);
    private DropSchemaDdlExecutor dropSchemaDdlExecutor;
    private DdlRequestContext context;
    private String schema;

    @BeforeEach
    void setUp() throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        dropSchemaDdlExecutor = new DropSchemaDdlExecutor(metadataExecutor,
                hotDeltaCacheService,
                okDeltaCacheService,
                entityCacheService,
                serviceDbFacade,
                evictQueryTemplateCacheService);
        doNothing().when(evictQueryTemplateCacheService).evictByDatamartName(anyString());
        schema = "shares";
        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schema);
        final String dropSchemaSql = "drop database shares";
        queryRequest.setSql(dropSchemaSql);
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);
        context.setDatamartName(schema);
    }

    @Test
    void executeSuccess() {
        Promise<QueryResult> promise = Promise.promise();
        when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture(true));

        when(metadataExecutor.execute(any()))
                .thenReturn(Future.succeededFuture());

        when(datamartDao.deleteDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture());

        dropSchemaDdlExecutor.execute(context, null)
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
        verify(evictQueryTemplateCacheService, times(2)).evictByDatamartName(schema);
    }

    @Test
    void executeWithDropSchemaError() {
        Promise<QueryResult> promise = Promise.promise();
        when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture(true));

        when(metadataExecutor.execute(any()))
                .thenReturn(Future.failedFuture(new DtmException("")));

        dropSchemaDdlExecutor.execute(context, null)
                .onComplete(promise);
        assertTrue(promise.future().failed());
        verify(evictQueryTemplateCacheService, times(1)).evictByDatamartName(any());
    }

    @Test
    void executeWithDropDatamartError() {
        Promise<QueryResult> promise = Promise.promise();
        when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture(true));

        when(metadataExecutor.execute(any()))
                .thenReturn(Future.succeededFuture());

        when(datamartDao.deleteDatamart(eq(schema)))
                .thenReturn(Future.failedFuture("delete datamart error"));

        dropSchemaDdlExecutor.execute(context, null)
                .onComplete(promise);
        assertTrue(promise.future().failed());
        verify(evictQueryTemplateCacheService, times(1)).evictByDatamartName(any());
    }

    @Test
    void executeDropInformationSchema() {
        schema = InformationSchemaView.SCHEMA_NAME.toLowerCase();
        when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture(true));
        when(metadataExecutor.execute(any()))
                .thenReturn(Future.succeededFuture());
        when(datamartDao.deleteDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture());
        dropSchemaDdlExecutor.execute(context, null)
                .onComplete(ar -> assertTrue(ar.failed()));
    }

}
