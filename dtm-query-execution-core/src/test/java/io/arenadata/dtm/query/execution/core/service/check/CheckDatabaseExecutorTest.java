package io.arenadata.dtm.query.execution.core.service.check;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.calcite.core.extension.check.CheckType;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dto.check.CheckContext;
import io.arenadata.dtm.query.execution.core.factory.CheckQueryResultFactory;
import io.arenadata.dtm.query.execution.core.factory.impl.check.CheckQueryResultFactoryImpl;
import io.arenadata.dtm.query.execution.core.service.check.impl.CheckDatabaseExecutor;
import io.arenadata.dtm.query.execution.core.service.datasource.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.datasource.impl.DataSourcePluginServiceImpl;
import io.vertx.core.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class CheckDatabaseExecutorTest {
    private final static String DATAMART_MNEMONIC = "schema";
    private final static Set<SourceType> SOURCE_TYPES = Stream.of(SourceType.ADB, SourceType.ADG, SourceType.ADQM)
            .collect(Collectors.toSet());

    private final DataSourcePluginService dataSourcePluginService = mock(DataSourcePluginServiceImpl.class);
    private final EntityDao entityDao = mock(EntityDao.class);
    private final DatamartDao datamartDao = mock(DatamartDao.class);
    private final CheckQueryResultFactory queryResultFactory = mock(CheckQueryResultFactoryImpl.class);
    private final CheckDatabaseExecutor checkDatabaseExecutor = new CheckDatabaseExecutor(dataSourcePluginService,
            entityDao, datamartDao, queryResultFactory);

    @BeforeEach
    void setUp() {
        when(dataSourcePluginService.getSourceTypes()).thenReturn(SOURCE_TYPES);
        when(dataSourcePluginService.checkTable(any(), any(), any())).thenReturn(Future.succeededFuture());
        when(datamartDao.getDatamart(DATAMART_MNEMONIC)).thenReturn(Future.succeededFuture(new byte[10]));

        Entity entity1 = Entity.builder()
                .schema(DATAMART_MNEMONIC)
                .entityType(EntityType.TABLE)
                .destination(SOURCE_TYPES)
                .name("entity1")
                .build();
        Entity entity2 = entity1.toBuilder()
                .name("entity2")
                .build();
        when(entityDao.getEntityNamesByDatamart(DATAMART_MNEMONIC))
                .thenReturn(Future.succeededFuture(Arrays.asList(entity1.getName(), entity2.getName())));
        when(entityDao.getEntity(DATAMART_MNEMONIC, entity1.getName()))
                .thenReturn(Future.succeededFuture(entity1));
        when(entityDao.getEntity(DATAMART_MNEMONIC, entity2.getName()))
                .thenReturn(Future.succeededFuture(entity2));
    }

    @Test
    void testSuccess() {
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic(DATAMART_MNEMONIC);
        CheckContext checkContext = new CheckContext(new RequestMetrics(), "env",
                new DatamartRequest(queryRequest), CheckType.DATABASE, null);
        checkDatabaseExecutor.execute(checkContext)
                .onComplete(ar -> assertTrue(ar.succeeded()));
        SOURCE_TYPES.forEach(sourceType -> {
            verify(dataSourcePluginService, times(1)).checkTable(eq(sourceType), any(),
                    argThat(request -> request.getEntity().getName().equals("entity1")));
            verify(dataSourcePluginService, times(1)).checkTable(eq(sourceType), any(),
                    argThat(request -> request.getEntity().getName().equals("entity2")));
        });
        verify(dataSourcePluginService, times(SOURCE_TYPES.size() * 2))
                .checkTable(any(), any(), any());
    }
}
