package io.arenadata.dtm.query.execution.core.service.ddl;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.cache.QueryTemplateKey;
import io.arenadata.dtm.common.cache.SourceQueryTemplateValue;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.core.service.ddl.impl.EvictCacheDdlPostExecutor;
import io.arenadata.dtm.query.execution.core.service.schema.impl.SystemDatamartViewsProviderImpl;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlType;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import org.apache.calcite.util.Pair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class EvictCacheDdlPostExecutorTest {
    private static final String USED_SCHEMA = "used_schema";
    private static final String USED_SCHEMA_TABLE = "used_schema_table";
    private static final String USED_SCHEMA_VIEW = "used_schema_view";
    private static final String TEMPLATE_1 = "template_1";
    private static final String TEMPLATE_2 = "template_2";
    private static final String TEMPLATE_3 = "template_3";
    private static final String TEMPLATE_4 = "template_4";
    private static final String TEMPLATE_5 = "template_5";
    private static final List<QueryTemplateKey> CACHE_LIST = Arrays.asList(
            getTemplate(TEMPLATE_1, USED_SCHEMA,
                    Collections.singletonList(getEntity(USED_SCHEMA_TABLE, EntityType.TABLE))),
            getTemplate(TEMPLATE_2, USED_SCHEMA,
                    Collections.singletonList(getEntity("table_1", EntityType.TABLE))),
            getTemplate(TEMPLATE_3, USED_SCHEMA,
                    Collections.singletonList(getEntity(USED_SCHEMA_VIEW, EntityType.VIEW))),
            getTemplate(TEMPLATE_4, USED_SCHEMA,
                    Arrays.asList(getEntity(USED_SCHEMA_VIEW, EntityType.VIEW),
                            getEntity(USED_SCHEMA_TABLE, EntityType.TABLE))),
            getTemplate(TEMPLATE_5, "not_used_schema",
                    Arrays.asList(getEntity(USED_SCHEMA_TABLE, "not_used_schema", EntityType.TABLE),
                            getEntity(USED_SCHEMA_VIEW, "not_used_schema", EntityType.VIEW)))
    );
    private final CacheService<QueryTemplateKey, SourceQueryTemplateValue> cacheService = mock(CacheService.class);
    private final EvictCacheDdlPostExecutor evictCacheDdlPostExecutor = new EvictCacheDdlPostExecutor(cacheService);

    @BeforeEach
    void init() {
        doNothing().when(cacheService).removeIf(any());
    }

    @Test
    void testCreateSchema() {
        checkNotExecute(DdlType.CREATE_SCHEMA);
    }

    @Test
    void testDropSchema() {
        DdlRequestContext context = getContext(DdlType.DROP_SCHEMA, null);
        validate(context, Arrays.asList(TEMPLATE_1, TEMPLATE_2, TEMPLATE_3, TEMPLATE_4));
    }

    @Test
    void testCreateTable() {
        checkNotExecute(DdlType.CREATE_TABLE);
    }

    @Test
    void testDropTable() {
        Entity entity = getEntity(USED_SCHEMA_TABLE, EntityType.TABLE);
        DdlRequestContext context = getContext(DdlType.DROP_TABLE, entity);
        validate(context, Arrays.asList(TEMPLATE_1, TEMPLATE_4));
    }

    @Test
    void testCreateView() {
        checkNotExecute(DdlType.CREATE_VIEW);
    }

    @Test
    void testDropView() {
        DdlRequestContext context = getContext(DdlType.DROP_VIEW, getEntity(USED_SCHEMA_VIEW, EntityType.VIEW));
        validate(context, Arrays.asList(TEMPLATE_3, TEMPLATE_4));
    }

    @Test
    void testUnknown() {
        checkNotExecute(DdlType.UNKNOWN);
    }

    private void checkNotExecute(DdlType type) {
        DdlRequestContext context = getContext(type, null);
        evictCacheDdlPostExecutor.execute(context);
        verify(cacheService, times(0)).removeIf(any());
    }

    private void validate(DdlRequestContext context, List<String> expDeletedTemplates) {
        evictCacheDdlPostExecutor.execute(context);
        ArgumentCaptor<Predicate<QueryTemplateKey>> captor = ArgumentCaptor.forClass(Predicate.class);
        verify(cacheService, times(1)).removeIf(captor.capture());
        List<String> deletedTemplates = CACHE_LIST.stream()
                .filter(captor.getValue())
                .map(QueryTemplateKey::getSourceQueryTemplate)
                .collect(Collectors.toList());
        assertEquals(expDeletedTemplates, deletedTemplates);
    }

    private DdlRequestContext getContext(DdlType type, Entity entity) {
        DdlRequestContext result = new DdlRequestContext(new DdlRequest(new QueryRequest(), entity));
        result.setDatamartName(USED_SCHEMA);
        result.setDdlType(type);
        return result;
    }

    private static QueryTemplateKey getTemplate(String template, String datamart, List<Entity> entities) {
        return QueryTemplateKey
                .builder()
                .sourceQueryTemplate(template)
                .logicalSchema(Collections.singletonList(new Datamart(datamart, false, entities)))
                .build();
    }

    private static Entity getEntity(String name, EntityType type) {
        return getEntity(name, USED_SCHEMA, type);
    }

    private static Entity getEntity(String name, String schema, EntityType type) {
        return Entity.builder()
                .name(name)
                .entityType(type)
                .schema(schema)
                .fields(Collections.emptyList())
                .build();
    }
}
