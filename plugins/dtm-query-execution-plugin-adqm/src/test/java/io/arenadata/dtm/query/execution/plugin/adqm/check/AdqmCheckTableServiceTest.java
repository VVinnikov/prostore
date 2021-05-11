package io.arenadata.dtm.query.execution.plugin.adqm.check;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.execution.plugin.adqm.check.service.AdqmCheckTableService;
import io.arenadata.dtm.query.execution.plugin.adqm.base.factory.AdqmMetaTableEntityFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.base.factory.AdqmTableEntitiesFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adqm.query.service.AdqmQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.adqm.base.utils.AdqmDdlUtil;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckTableRequest;
import io.arenadata.dtm.query.execution.plugin.api.factory.MetaTableEntityFactory;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckTableService;
import io.vertx.core.Future;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adqm.base.utils.Constants.ACTUAL_POSTFIX;
import static io.arenadata.dtm.query.execution.plugin.adqm.base.utils.Constants.ACTUAL_SHARD_POSTFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AdqmCheckTableServiceTest {
    private static final List<String> tablePostFixes = Arrays.asList(ACTUAL_SHARD_POSTFIX, ACTUAL_POSTFIX);
    private static final String TEST_COLUMN_NAME = "test_column";
    private static final String ENV = "env";
    private static Map<String, List<Map<String, Object>>> sysColumns;
    private Entity entity;
    private CheckTableService adqmCheckTableService;
    private CheckTableRequest checkTableRequest;

    @BeforeAll
    static void init() {
        sysColumns = new HashMap<>();
        sysColumns.put(ACTUAL_SHARD_POSTFIX, Arrays.asList(
                getMapColumn("sys_from", "Int64", true),
                getMapColumn("sys_to", "Int64", false),
                getMapColumn("sys_op", "Int8", false),
                getMapColumn("sys_close_date", "DateTime", false),
                getMapColumn("sign", "Int8", false)
        ));
        sysColumns.put(ACTUAL_POSTFIX, Arrays.asList(
                getMapColumn("sys_from", "Int64", false),
                getMapColumn("sys_to", "Int64", false),
                getMapColumn("sys_op", "Int8", false),
                getMapColumn("sys_close_date", "DateTime", false),
                getMapColumn("sign", "Int8", false)
        ));
    }

    @BeforeEach
    void setUp() {
        DatabaseExecutor adqmQueryExecutor = mock(AdqmQueryExecutor.class);
        entity = getEntity();
        int fieldsCount = entity.getFields().size();
        entity.getFields().add(EntityField.builder()
                .name(TEST_COLUMN_NAME)
                .ordinalPosition(fieldsCount + 1)
                .type(ColumnType.BIGINT)
                .nullable(true)
                .build());

        checkTableRequest = new CheckTableRequest(UUID.randomUUID(), ENV, entity.getSchema(), entity);

        tablePostFixes.forEach(postFix -> when(adqmQueryExecutor.execute(argThat(getPredicate(postFix)::test)))
                .thenReturn(Future.succeededFuture(getResultSet(postFix, entity.getFields()))));

        List<String> queries = tablePostFixes.stream()
                .map(postFix -> String.format(AdqmMetaTableEntityFactory.QUERY_PATTERN,
                        String.format("%s%s", entity.getName(), postFix), ENV, entity.getSchema()))
                .collect(Collectors.toList());
        when(adqmQueryExecutor.execute(argThat(arg -> queries.stream().noneMatch(arg::equals))))
                .thenReturn(Future.succeededFuture(Collections.emptyList()));
        adqmCheckTableService = new AdqmCheckTableService(new AdqmTableEntitiesFactory(),
                new AdqmMetaTableEntityFactory(adqmQueryExecutor));
    }

    @Test
    void testSuccess() {
        assertTrue(adqmCheckTableService.check(checkTableRequest).succeeded());
    }

    @Test
    void testTableNotExist() {
        entity.setName("not_exist_table");
        String errors = adqmCheckTableService.check(checkTableRequest).cause().getMessage();
        tablePostFixes.forEach(postFix ->
                assertThat(errors, containsString(String.format(CheckTableService.TABLE_NOT_EXIST_ERROR_TEMPLATE,
                        String.format("not_exist_table%s", postFix)))));
    }

    @Test
    void testColumnNotExist() {
        entity.getFields().add(EntityField.builder()
                .name("not_exist_column")
                .size(1)
                .type(ColumnType.VARCHAR)
                .build());
        String expectedError = String.format(CheckTableService.COLUMN_NOT_EXIST_ERROR_TEMPLATE,
                "not_exist_column");

        assertThat(adqmCheckTableService.check(checkTableRequest).cause().getMessage(),
                containsString(expectedError));
    }

    @Test
    void testDataType() {
        String expectedError = String.format(CheckTableService.FIELD_ERROR_TEMPLATE,
                MetaTableEntityFactory.DATA_TYPE, "String", "Int64");
        testColumns(field -> field.setType(ColumnType.VARCHAR), expectedError);

    }

    @Test
    void testSortedKey() {
        String expectedError = String.format(AdqmCheckTableService.SORTED_KEY_ERROR_TEMPLATE,
                "id, pk2, test_column, sys_from", "id, pk2, sys_from");
        testColumns(field -> field.setPrimaryOrder(4), expectedError);

    }

    private void testColumns(Consumer<EntityField> consumer,
                             String expectedError) {
        EntityField testColumn = entity.getFields().stream()
                .filter(field -> TEST_COLUMN_NAME.equals(field.getName()))
                .findAny()
                .orElseThrow(RuntimeException::new);
        consumer.accept(testColumn);
        assertThat(adqmCheckTableService.check(checkTableRequest).cause().getMessage(),
                containsString(expectedError));
    }

    private Predicate<String> getPredicate(String postFix) {
        String query = String.format(AdqmMetaTableEntityFactory.QUERY_PATTERN,
                String.format("%s%s", entity.getName(), postFix), ENV, entity.getSchema());
        return query::equals;
    }

    private Map<String, Object> fieldToMapTransform(EntityField field, String postFix) {
        HashMap<String, Object> result = new HashMap<>();
        Function<Integer, Integer> sortKeyFunc = val -> ACTUAL_SHARD_POSTFIX.equals(postFix) ? 1 : 0;
        result.put(AdqmMetaTableEntityFactory.IS_IN_SORTING_KEY, Optional.ofNullable(field.getPrimaryOrder())
                .map(sortKeyFunc)
                .orElse(0));
        result.put(AdqmMetaTableEntityFactory.COLUMN_NAME, field.getName());
        result.put(AdqmMetaTableEntityFactory.DATA_TYPE, AdqmDdlUtil.classTypeToNative(field.getType()));
        return result;
    }

    private static Map<String, Object> getMapColumn(String name, String dataType, boolean isInSortingKeys) {
        Map<String, Object> result = new HashMap<>();
        result.put(AdqmMetaTableEntityFactory.COLUMN_NAME, name);
        result.put(AdqmMetaTableEntityFactory.DATA_TYPE, dataType);
        result.put(AdqmMetaTableEntityFactory.IS_IN_SORTING_KEY, isInSortingKeys ? 1 : 0);
        return result;
    }

    private List<Map<String, Object>> getResultSet(String postfix, List<EntityField> fields) {
        List<Map<String, Object>> result = fields.stream()
                .map(field -> fieldToMapTransform(field, postfix))
                .collect(Collectors.toList());
        result.addAll(sysColumns.get(postfix));
        return result;
    }

    public static Entity getEntity() {
        List<EntityField> keyFields = Arrays.asList(
                new EntityField(0, "id", ColumnType.INT.name(), false, 1, 1, null),
                new EntityField(1, "sk_key2", ColumnType.INT.name(), false, null, 2, null),
                new EntityField(2, "pk2", ColumnType.INT.name(), false, 2, null, null),
                new EntityField(3, "sk_key3", ColumnType.INT.name(), false, null, 3, null)
        );
        ColumnType[] types = ColumnType.values();
        List<EntityField> fields = new ArrayList<>();
        for (int i = 0; i < types.length; i++) {
            ColumnType type = types[i];
            if (Arrays.asList(ColumnType.BLOB, ColumnType.ANY).contains(type)) {
                continue;
            }

            EntityField.EntityFieldBuilder builder = EntityField.builder()
                    .ordinalPosition(i + keyFields.size())
                    .type(type)
                    .nullable(true)
                    .name(type.name() + "_type");
            if (Arrays.asList(ColumnType.CHAR, ColumnType.VARCHAR).contains(type)) {
                builder.size(20);
            } else if (Arrays.asList(ColumnType.TIME, ColumnType.TIMESTAMP).contains(type)) {
                builder.accuracy(5);
            }
            fields.add(builder.build());
        }
        fields.addAll(keyFields);
        return new Entity("test_schema.test_table", fields);
    }
}
