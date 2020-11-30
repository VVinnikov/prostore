package io.arenadata.dtm.query.execution.plugin.adg.service.impl.check;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.plugin.adg.configuration.properties.TarantoolDatabaseProperties;
import io.arenadata.dtm.query.execution.plugin.adg.factory.impl.AdgCreateTableQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.adg.factory.impl.AdgTableEntitiesFactory;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.schema.*;
import io.arenadata.dtm.query.execution.plugin.adg.service.AdgCartridgeClient;
import io.arenadata.dtm.query.execution.plugin.adg.service.impl.AdgCartridgeClientImpl;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckTableService;
import io.vertx.core.Future;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.AdditionalMatchers;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adg.constants.ColumnFields.*;
import static io.arenadata.dtm.query.execution.plugin.adg.factory.impl.AdgTableEntitiesFactory.SEC_INDEX_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AdgCheckTableServiceTest {
    private static final String TEST_COLUMN_NAME = "test_column";
    private static final String NOT_TABLE_EXIST = "not_exist_table";
    private static final String ENV = "env";
    private static Set<String> spacePostfixes;
    private static Map<String, List<SpaceIndex>> spaceIndexMap;
    private Entity entity;
    private CheckContext checkContext;
    private CheckTableService adgCheckTableService;

    @BeforeAll
    static void init() {
        spaceIndexMap = new HashMap<>();
        spaceIndexMap.put(ACTUAL_POSTFIX, Arrays.asList(
                new SpaceIndex(true, Collections.emptyList(), SpaceIndexTypes.TREE, ID),
                new SpaceIndex(false, Collections.emptyList(), SpaceIndexTypes.TREE, SEC_INDEX_PREFIX + SYS_FROM_FIELD),
                new SpaceIndex(false, Collections.emptyList(), SpaceIndexTypes.TREE, BUCKET_ID)
        ));
        spaceIndexMap.put(HISTORY_POSTFIX, Arrays.asList(
                new SpaceIndex(true, Collections.emptyList(), SpaceIndexTypes.TREE, ID),
                new SpaceIndex(false, Collections.emptyList(), SpaceIndexTypes.TREE, SEC_INDEX_PREFIX + SYS_FROM_FIELD),
                new SpaceIndex(false, Collections.emptyList(), SpaceIndexTypes.TREE, SEC_INDEX_PREFIX + SYS_TO_FIELD),
                new SpaceIndex(false, Collections.emptyList(), SpaceIndexTypes.TREE, BUCKET_ID)
        ));
        spaceIndexMap.put(STAGING_POSTFIX, Arrays.asList(
                new SpaceIndex(true, Collections.emptyList(), SpaceIndexTypes.TREE, ID),
                new SpaceIndex(false, Collections.emptyList(), SpaceIndexTypes.TREE, BUCKET_ID)
        ));

        spacePostfixes = new HashSet<>();
        spacePostfixes.add(ACTUAL_POSTFIX);
        spacePostfixes.add(HISTORY_POSTFIX);
        spacePostfixes.add(STAGING_POSTFIX);
    }

    @BeforeEach
    void setUp() {

        AdgCartridgeClient adgClient = mock(AdgCartridgeClientImpl.class);
        entity = getEntity();
        int fieldsCount = entity.getFields().size();
        entity.getFields().add(EntityField.builder()
                .name(TEST_COLUMN_NAME)
                .ordinalPosition(fieldsCount + 1)
                .type(ColumnType.BIGINT)
                .nullable(true)
                .build());

        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setEnvName(ENV);
        queryRequest.setDatamartMnemonic(entity.getSchema());
        checkContext = new CheckContext(null, new DatamartRequest(queryRequest), entity);

        Map<String, Space> spaces = getSpaces(entity);
        when(adgClient.getSpaceDescriptions(eq(spaces.keySet())))
                .thenReturn(Future.succeededFuture(spaces));
        when(adgClient.getSpaceDescriptions(AdditionalMatchers.not(eq(spaces.keySet()))))
                .thenReturn(Future.failedFuture(String.format(CheckTableService.TABLE_NOT_EXIST_ERROR_TEMPLATE,
                        NOT_TABLE_EXIST + ACTUAL_POSTFIX)));

        adgCheckTableService = new AdgCheckTableService(adgClient,
                new AdgCreateTableQueriesFactory(new AdgTableEntitiesFactory(new TarantoolDatabaseProperties())));
    }

    @Test
    void testSuccess() {
        assertTrue(adgCheckTableService.check(checkContext).succeeded());
    }

    @Test
    void testTableNotExist() {
        entity.setName("not_exist_table");
        assertThat(adgCheckTableService.check(checkContext).cause().getMessage(),
                containsString(String.format(CheckTableService.TABLE_NOT_EXIST_ERROR_TEMPLATE,
                        NOT_TABLE_EXIST + ACTUAL_POSTFIX)));
    }

    @Test
    void testColumnNotExist() {
        entity.getFields().add(EntityField.builder()
                .name("not_exist_column")
                .size(1)
                .type(ColumnType.VARCHAR)
                .build());
        String expectedError = String.format(AdgCheckTableService.COLUMN_NOT_EXIST_ERROR_TEMPLATE,
                "not_exist_column");
        assertThat(adgCheckTableService.check(checkContext).cause().getMessage(),
                containsString(expectedError));
    }

    @Test
    void testDataType() {
        String expectedError = String.format(CheckTableService.FIELD_ERROR_TEMPLATE,
                CheckTableService.DATA_TYPE, "string", "integer");
        testColumns(field -> field.setType(ColumnType.VARCHAR), expectedError);

    }

    private void testColumns(Consumer<EntityField> consumer,
                             String expectedError) {
        EntityField testColumn = entity.getFields().stream()
                .filter(field -> TEST_COLUMN_NAME.equals(field.getName()))
                .findAny()
                .orElseThrow(RuntimeException::new);
        consumer.accept(testColumn);
        assertThat(adgCheckTableService.check(checkContext).cause().getMessage(),
                containsString(expectedError));
    }

    private Map<String, Space> getSpaces(Entity entity) {
        List<SpaceAttribute> logAttrs = entity.getFields().stream()
                .map(field -> new SpaceAttribute(field.getNullable(), field.getName(),
                        SpaceAttributeTypeUtil.toAttributeType(field.getType())))
                .collect(Collectors.toList());
        return spacePostfixes.stream()
                .collect(Collectors.toMap(
                        postfix -> String.format("env__%s__%s%s", entity.getSchema(), entity.getName(), postfix),
                        postfix -> Space.builder()
                                .format(getAttrs(postfix, logAttrs))
                                .indexes(spaceIndexMap.get(postfix))
                                .build()));
    }

    private List<SpaceAttribute> getAttrs(String postfix, List<SpaceAttribute> logAttrs) {
        List<SpaceAttribute> result = new ArrayList<>();
        result.add(new SpaceAttribute(false, BUCKET_ID, SpaceAttributeTypes.UNSIGNED));
        switch (postfix) {
            case ACTUAL_POSTFIX:
            case HISTORY_POSTFIX:
                result.add(new SpaceAttribute(false, SYS_FROM_FIELD, SpaceAttributeTypes.NUMBER));
                result.add(new SpaceAttribute(true, SYS_TO_FIELD, SpaceAttributeTypes.NUMBER));
                result.add(new SpaceAttribute(false, SYS_OP_FIELD, SpaceAttributeTypes.NUMBER));
            case STAGING_POSTFIX:
                result.add(new SpaceAttribute(false, SYS_OP_FIELD, SpaceAttributeTypes.NUMBER));
        }
        result.addAll(logAttrs);
        return result;
    }

    private static Entity getEntity() {
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
