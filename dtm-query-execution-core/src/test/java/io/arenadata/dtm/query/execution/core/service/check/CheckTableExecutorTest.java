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
package io.arenadata.dtm.query.execution.core.service.check;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.calcite.core.extension.check.CheckType;
import io.arenadata.dtm.query.calcite.core.extension.check.SqlCheckTable;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dto.check.CheckContext;
import io.arenadata.dtm.query.execution.core.service.check.impl.CheckTableExecutor;
import io.arenadata.dtm.query.execution.core.service.datasource.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.datasource.impl.DataSourcePluginServiceImpl;
import io.vertx.core.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

public class CheckTableExecutorTest {
    private final static String DATAMART_MNEMONIC = "schema";
    private final static Set<SourceType> SOURCE_TYPES = Stream.of(SourceType.ADB, SourceType.ADG, SourceType.ADQM)
            .collect(Collectors.toSet());

    private final DataSourcePluginService dataSourcePluginService = mock(DataSourcePluginServiceImpl.class);
    private final EntityDao entityDao = mock(EntityDao.class);
    private final CheckTableExecutor checkTableExecutor = new CheckTableExecutor(dataSourcePluginService, entityDao);
    private Entity entity;


    @BeforeEach
    void setUp() {
        when(dataSourcePluginService.getSourceTypes()).thenReturn(SOURCE_TYPES);
        when(dataSourcePluginService.checkTable(any(), any(), any())).thenReturn(Future.succeededFuture());

        entity = Entity.builder()
                .schema(DATAMART_MNEMONIC)
                .entityType(EntityType.TABLE)
                .destination(SOURCE_TYPES)
                .name("entity")
                .build();
        when(entityDao.getEntity(DATAMART_MNEMONIC, entity.getName()))
                .thenReturn(Future.succeededFuture(entity));
    }

    @Test
    void testSuccess() {
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic(DATAMART_MNEMONIC);
        SqlCheckTable sqlCheckTable = mock(SqlCheckTable.class);
        when(sqlCheckTable.getTable()).thenReturn(entity.getName());
        CheckContext checkContext = new CheckContext(new RequestMetrics(), "env",
                new DatamartRequest(queryRequest), CheckType.TABLE, sqlCheckTable);
        checkTableExecutor.execute(checkContext)
                .onComplete(ar -> assertTrue(ar.succeeded()));
        SOURCE_TYPES.forEach(sourceType -> verify(dataSourcePluginService, times(1))
                .checkTable(eq(sourceType), any(),
                        argThat(request -> request.getEntity().getName().equals(entity.getName()))));
        verify(dataSourcePluginService, times(SOURCE_TYPES.size())).checkTable(any(), any(), any());
    }
}
