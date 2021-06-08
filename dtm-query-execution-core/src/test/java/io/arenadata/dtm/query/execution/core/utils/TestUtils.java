package io.arenadata.dtm.query.execution.core.utils;

import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.dialect.LimitSqlDialect;
import io.arenadata.dtm.query.calcite.core.provider.CalciteContextProvider;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.execution.core.base.configuration.AppConfiguration;
import io.arenadata.dtm.query.execution.core.base.configuration.properties.CoreDtmSettings;
import io.arenadata.dtm.query.execution.core.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.calcite.service.CoreCalciteDefinitionService;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TestUtils {
    public static final CalciteConfiguration CALCITE_CONFIGURATION = new CalciteConfiguration();
    public static final CalciteCoreConfiguration CALCITE_CORE_CONFIGURATION = new CalciteCoreConfiguration();
    public static final DefinitionService<SqlNode> DEFINITION_SERVICE =
            new CoreCalciteDefinitionService(CALCITE_CONFIGURATION.configEddlParser(CALCITE_CORE_CONFIGURATION.eddlParserImplFactory()));
    public static final SqlDialect SQL_DIALECT = new LimitSqlDialect(SqlDialect.EMPTY_CONTEXT);
    public static final CoreDtmSettings CORE_DTM_SETTINGS = new CoreDtmSettings(ZoneId.of("UTC"));

    private static final LimitSqlDialect SQL_DIALECT_DEFAULT_CONTEXT = new LimitSqlDialect(CalciteSqlDialect.DEFAULT_CONTEXT);

    private TestUtils() {
    }

    public static AppConfiguration getCoreConfiguration(String envName) {
        return getCoreAppConfiguration(CORE_DTM_SETTINGS, envName);
    }

    public static AppConfiguration getCoreAppConfiguration(DtmConfig dtmSettings, String envName) {
        return new AppConfiguration(null) {
            @Override
            public String getEnvName() {
                return envName;
            }

            @Override
            public DtmConfig dtmSettings() {
                return dtmSettings;
            }
        };
    }

    public static void assertException(Class<? extends Throwable> expected, String partOfMessage, Throwable actual) {
        assertNotNull(actual);
        assertSame(expected, actual.getClass());
        assertTrue(actual.getMessage().contains(partOfMessage), String.format("Message: %s\nNot contains expected part of message: %s", actual.getMessage(), partOfMessage));
    }

    public static void initEntityList(List<Entity> entityList, String schema) {
        List<EntityField> fields = Collections.singletonList(
                EntityField.builder()
                        .ordinalPosition(0)
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .nullable(false)
                        .build());
        Entity entity1 = Entity.builder()
                .schema(schema)
                .name("test_view")
                .viewQuery(String.format("SELECT * FROM %s.%s", schema, "test_table"))
                .fields(fields)
                .entityType(EntityType.VIEW)
                .build();
        Entity entity2 = Entity.builder()
                .schema(schema)
                .name("test_table")
                .fields(fields)
                .entityType(EntityType.TABLE)
                .build();
        Entity entity3 = Entity.builder()
                .schema(schema)
                .name("accounts")
                .fields(fields)
                .entityType(EntityType.TABLE)
                .build();
        Entity entity4 = Entity.builder()
                .schema(schema)
                .name("transactions")
                .fields(fields)
                .entityType(EntityType.TABLE)
                .build();

        List<EntityField> accountFields = new ArrayList<>();
        accountFields.add(EntityField.builder()
                .ordinalPosition(0)
                .name("id")
                .type(ColumnType.BIGINT)
                .nullable(false)
                .build());
        accountFields.add(EntityField.builder()
                .ordinalPosition(0)
                .name("account_type")
                .type(ColumnType.BIGINT)
                .nullable(false)
                .build());
        Entity entity5 = Entity.builder()
                .schema(schema)
                .name("accounts1")
                .fields(accountFields)
                .entityType(EntityType.TABLE)
                .build();
        Entity entity6 = Entity.builder()
                .schema(schema)
                .name("accounts2")
                .fields(accountFields)
                .entityType(EntityType.TABLE)
                .build();

        entityList.add(entity1);
        entityList.add(entity2);
        entityList.add(entity3);
        entityList.add(entity4);
        entityList.add(entity5);
        entityList.add(entity6);
    }


    @SneakyThrows
    public static QueryParserResponse parse(CalciteContextProvider contextProvider, QueryParserRequest request) {
        val context = contextProvider.context(request.getSchema());
        val sql = request.getQuery().toSqlString(SQL_DIALECT_DEFAULT_CONTEXT).getSql();
        val parse = context.getPlanner().parse(sql);
        val validatedQuery = context.getPlanner().validate(parse);
        val relQuery = context.getPlanner().rel(validatedQuery);
        return new QueryParserResponse(
                context,
                request.getSchema(),
                relQuery,
                validatedQuery);
    }
}
