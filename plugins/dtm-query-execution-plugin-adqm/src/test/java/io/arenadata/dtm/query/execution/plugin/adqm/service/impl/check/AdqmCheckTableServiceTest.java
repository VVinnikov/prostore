package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.check;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.common.DdlUtils;
import io.arenadata.dtm.query.execution.plugin.adqm.factory.impl.AdqmCreateTableQueriesFactoryTest;
import io.arenadata.dtm.query.execution.plugin.adqm.factory.impl.AdqmTableEntitiesFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adqm.service.impl.query.AdqmQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.CheckTableService;
import io.vertx.core.Future;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adqm.common.Constants.ACTUAL_POSTFIX;
import static io.arenadata.dtm.query.execution.plugin.adqm.common.Constants.ACTUAL_SHARD_POSTFIX;
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
    private CheckTableService adbCheckTableService;
    private DatamartRequest request;

    @BeforeAll
    static void init() {
        sysColumns = new HashMap<>();
        sysColumns.put(ACTUAL_SHARD_POSTFIX, Arrays.asList(
                getMapColumn("sys_from", "Int64", true),
                getMapColumn("sys_to", "Int64", false),
                getMapColumn("sys_op", "Int8", false),
                getMapColumn("close_date", "DateTime", false),
                getMapColumn("sign", "Int8", false)
        ));
        sysColumns.put(ACTUAL_POSTFIX, Arrays.asList(
                getMapColumn("sys_from", "Int64", false),
                getMapColumn("sys_to", "Int64", false),
                getMapColumn("sys_op", "Int8", false),
                getMapColumn("close_date", "DateTime", false),
                getMapColumn("sign", "Int8", false)
        ));
    }

    @BeforeEach
    void setUp() {
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setEnvName(ENV);
        request = new DatamartRequest(queryRequest);
        DatabaseExecutor adqmQueryExecutor = mock(AdqmQueryExecutor.class);
        entity = AdqmCreateTableQueriesFactoryTest.getEntity();
        int fieldsCount = entity.getFields().size();
        entity.getFields().add(EntityField.builder()
                .name(TEST_COLUMN_NAME)
                .ordinalPosition(fieldsCount + 1)
                .type(ColumnType.BIGINT)
                .nullable(true)
                .build());

        tablePostFixes.forEach(postFix -> when(adqmQueryExecutor.execute(argThat(getPredicate(postFix)::test)))
                .thenReturn(Future.succeededFuture(getResultSet(postFix, entity.getFields()))));

        List<String> queries = tablePostFixes.stream()
                .map(postFix -> String.format(AdqmCheckTableService.QUERY_PATTERN,
                        String.format("%s%s", entity.getName(), postFix), ENV, entity.getSchema()))
                .collect(Collectors.toList());
        when(adqmQueryExecutor.execute(argThat(arg -> queries.stream().noneMatch(arg::equals))))
                .thenReturn(Future.succeededFuture(Collections.emptyList()));
        adbCheckTableService = new AdqmCheckTableService(adqmQueryExecutor, new AdqmTableEntitiesFactory());
    }

    @Test
    void testSuccess() {
        adbCheckTableService.check(new CheckContext(request, entity),
                ar -> assertTrue(ar.succeeded()));
    }

    @Test
    void testTableNotExist() {
        entity.setName("not_exist_table");
        adbCheckTableService.check(new CheckContext(request, entity),
                ar -> tablePostFixes.forEach(postFix ->
                        assertThat(ar.cause().getMessage(),
                                containsString(String.format(CheckTableService.TABLE_NOT_EXIST_ERROR_TEMPLATE,
                                        String.format("not_exist_table%s", postFix))))));
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
        adbCheckTableService.check(new CheckContext(request, entity),
                ar -> assertThat(ar.cause().getMessage(), containsString(expectedError)));
    }

    @Test
    void testDataType() {
        String expectedError = String.format(CheckTableService.FIELD_ERROR_TEMPLATE,
                CheckTableService.DATA_TYPE, "String", "Int64");
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
        adbCheckTableService.check(new CheckContext(request, entity),
                ar -> assertThat(ar.cause().getMessage(), containsString(expectedError)));
    }

    private Predicate<String> getPredicate(String postFix) {
        String query = String.format(AdqmCheckTableService.QUERY_PATTERN,
                String.format("%s%s", entity.getName(), postFix), ENV, entity.getSchema());
        return query::equals;
    }

    private Map<String, Object> fieldToMapTransform(EntityField field, String postFix) {
        HashMap<String, Object> result = new HashMap<>();
        Function<Integer, Integer> sortKeyFunc = val -> ACTUAL_SHARD_POSTFIX.equals(postFix) ? 1 : 0;
        result.put(AdqmCheckTableService.IS_IN_SORTING_KEY, Optional.ofNullable(field.getPrimaryOrder())
                .map(sortKeyFunc)
                .orElse(0));
        result.put(CheckTableService.COLUMN_NAME, field.getName());
        result.put(CheckTableService.DATA_TYPE, DdlUtils.classTypeToNative(field.getType()));
        return result;
    }

    private static Map<String, Object> getMapColumn(String name, String dataType, boolean isInSortingKeys) {
        Map<String, Object> result = new HashMap<>();
        result.put(AdqmCheckTableService.COLUMN_NAME, name);
        result.put(AdqmCheckTableService.DATA_TYPE, dataType);
        result.put(AdqmCheckTableService.IS_IN_SORTING_KEY, isInSortingKeys ? 1 : 0);
        return result;
    }

    private List<Map<String, Object>> getResultSet(String postfix, List<EntityField> fields) {
        List<Map<String, Object>> result = fields.stream()
                .map(field -> fieldToMapTransform(field, postfix))
                .collect(Collectors.toList());
        result.addAll(sysColumns.get(postfix));
        return result;
    }
}
