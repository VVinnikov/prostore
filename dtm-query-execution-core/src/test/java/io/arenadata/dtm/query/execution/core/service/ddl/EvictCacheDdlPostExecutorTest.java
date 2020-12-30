package io.arenadata.dtm.query.execution.core.service.ddl;

import io.arenadata.dtm.cache.service.EvictQueryTemplateCacheService;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.core.service.ddl.impl.EvictCacheDdlPostExecutor;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlType;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.mockito.Mockito.*;

public class EvictCacheDdlPostExecutorTest {
    private final EvictQueryTemplateCacheService evictQueryTemplateCacheService =
            mock(EvictQueryTemplateCacheService.class);
    private final EvictCacheDdlPostExecutor evictCacheDdlPostExecutor =
            new EvictCacheDdlPostExecutor(evictQueryTemplateCacheService);

    @BeforeEach
    void init() {
        doNothing().when(evictQueryTemplateCacheService).evictByDatamartName(anyString());
        doNothing().when(evictQueryTemplateCacheService).evictByEntityName(anyString(), anyString());
    }

    @Test
    void testCreateSchema() {
        checkNotExecute(DdlType.CREATE_SCHEMA);
    }

    @Test
    void testDropSchema() {
        DdlRequestContext context = getContext(DdlType.DROP_SCHEMA, null);
        context.setDatamartName("schema");
        evictCacheDdlPostExecutor.execute(context);
        verify(evictQueryTemplateCacheService, times(1)).evictByDatamartName("schema");
        verify(evictQueryTemplateCacheService, times(0)).evictByEntityName(anyString(),
                anyString());
    }

    @Test
    void testCreateTable() {
        checkNotExecute(DdlType.CREATE_TABLE);
    }

    @Test
    void testDropTable() {
        Entity entity = getEntity(EntityType.TABLE);
        DdlRequestContext context = getContext(DdlType.DROP_TABLE, entity);
        evictCacheDdlPostExecutor.execute(context);
        verify(evictQueryTemplateCacheService, times(0)).evictByDatamartName(anyString());
        verify(evictQueryTemplateCacheService, times(1)).evictByEntityName(entity.getSchema(),
                entity.getName());
    }

    @Test
    void testCreateView() {
        checkNotExecute(DdlType.CREATE_VIEW);
    }

    @Test
    void testDropView() {
        checkNotExecute(DdlType.DROP_VIEW);
    }

    @Test
    void testUnknown() {
        checkNotExecute(DdlType.UNKNOWN);
    }

    private void checkNotExecute(DdlType type) {
        DdlRequestContext context = getContext(type, null);
        evictCacheDdlPostExecutor.execute(context);
        verify(evictQueryTemplateCacheService, times(0)).evictByDatamartName(anyString());
        verify(evictQueryTemplateCacheService, times(0)).evictByEntityName(anyString(),
                anyString());
    }

    private DdlRequestContext getContext(DdlType type, Entity entity) {
        DdlRequestContext result = new DdlRequestContext(new DdlRequest(new QueryRequest(), entity));
        result.setDatamartName("schema_name");
        result.setDdlType(type);
        return result;
    }

    private static Entity getEntity(EntityType type) {
        return Entity.builder()
                .name("entity_name")
                .entityType(type)
                .schema("schema_name")
                .fields(Collections.emptyList())
                .build();
    }
}
