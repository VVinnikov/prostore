package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.query;

import io.arenadata.dtm.calcite.adqm.configuration.AdqmCalciteConfiguration;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.calcite.core.framework.DtmCalciteFramework;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.service.AdqmQueryJoinConditionsCheckService;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AdqmQueryJoinConditionsCheckServiceImplTest {

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final AdqmCalciteConfiguration calciteCoreConfiguration = new AdqmCalciteConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration.configDdlParser(
            calciteCoreConfiguration.eddlParserImplFactory()
    );
    private AdqmQueryJoinConditionsCheckService conditionsCheckService;
    private String envName;
    private List<Datamart> schemas;

    @BeforeEach
    void setUp() {
        envName = "test";
        conditionsCheckService = new AdqmQueryJoinConditionsCheckServiceImpl();
        schemas = createSchema();
    }

    @Test
    void queryCondition() throws SqlParseException {
        String query = "SELECT * FROM dtm_1032.products\n" +
                "INNER JOIN dtm_1032.categories on dtm_1032.products.id = dtm_1032.categories.id\n" +
                "                                      AND dtm_1032.products.product_name = dtm_1032.categories.category_name\n" +
                "WHERE dtm_1032.products.category_id > 5\n" +
                "    limit 5";
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        SqlNode sqlNode = planner.parse(query);
        EnrichQueryRequest request = EnrichQueryRequest.builder()
                .query(sqlNode)
                .envName(envName)
                .schema(schemas)
                .build();
        assertTrue(conditionsCheckService.isJoinConditionsCorrect(request));
    }

    @Test
    void queryConditionDuplicate() throws SqlParseException {
        String query = "SELECT * FROM dtm_1032.products\n" +
                "INNER JOIN dtm_1032.categories on dtm_1032.products.id = dtm_1032.categories.id\n" +
                "                                      AND dtm_1032.products.product_name = dtm_1032.categories.category_name\n" +
                "                                      AND dtm_1032.products.id = dtm_1032.categories.id\n" +
                "WHERE dtm_1032.products.category_id > 5\n" +
                "    limit 5";
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        SqlNode sqlNode = planner.parse(query);
        EnrichQueryRequest request = EnrichQueryRequest.builder()
                .query(sqlNode)
                .envName(envName)
                .schema(schemas)
                .build();
        assertTrue(conditionsCheckService.isJoinConditionsCorrect(request));
    }

    @Test
    void queryConditionWithOr() throws SqlParseException {
        String query = "SELECT * FROM dtm_1032.products\n" +
                "INNER JOIN dtm_1032.categories on dtm_1032.products.id = dtm_1032.categories.id\n" +
                "                                      AND dtm_1032.products.product_name = dtm_1032.categories.category_name\n" +
                "                                      OR dtm_1032.products.name = dtm_1032.categories.name\n" +
                "WHERE dtm_1032.products.category_id > 5\n" +
                "    limit 5";
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        SqlNode sqlNode = planner.parse(query);
        EnrichQueryRequest request = EnrichQueryRequest.builder()
                .query(sqlNode)
                .envName(envName)
                .schema(schemas)
                .build();
        assertFalse(conditionsCheckService.isJoinConditionsCorrect(request));
    }

    @Test
    void queryConditionWithSeveralDistrKeys() throws SqlParseException {
        String query = "SELECT * FROM dtm_1032.accounts\n" +
                "INNER JOIN dtm_1032.sales on dtm_1032.accounts.id = dtm_1032.sales.id\n" +
                "                                      AND dtm_1032.accounts.account_id = dtm_1032.sales.account_id\n" +
                "                                      AND dtm_1032.accounts.name = dtm_1032.sales.name\n" +
                "WHERE dtm_1032.accounts.category_id > 5\n" +
                "    limit 5";

        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        SqlNode sqlNode = planner.parse(query);
        EnrichQueryRequest request = EnrichQueryRequest.builder()
                .query(sqlNode)
                .envName(envName)
                .schema(schemas)
                .build();
        assertTrue(conditionsCheckService.isJoinConditionsCorrect(request));
    }

    @Test
    void queryConditionWithIncorrectOrder() throws SqlParseException {
        String query = "SELECT * FROM dtm_1032.accounts\n" +
                "INNER JOIN dtm_1032.sales on dtm_1032.accounts.id = dtm_1032.accounts.account_id\n" +
                "                                      AND dtm_1032.accounts.account_id = dtm_1032.sales.id\n" +
                "                                      AND dtm_1032.accounts.name = dtm_1032.sales.name\n" +
                "WHERE dtm_1032.sales.category_id > 5\n" +
                "    limit 5";
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        SqlNode sqlNode = planner.parse(query);
        EnrichQueryRequest request = EnrichQueryRequest.builder()
                .query(sqlNode)
                .envName(envName)
                .schema(schemas)
                .build();
        assertFalse(conditionsCheckService.isJoinConditionsCorrect(request));
    }

    @Test
    void queryConditionWithNoDistrKeys() throws SqlParseException {
        String query = "SELECT * FROM dtm_1032.products\n" +
                "INNER JOIN dtm_1032.categories on dtm_1032.products.name = dtm_1032.categories.name\n" +
                "WHERE dtm_1032.products.category_id > 5\n" +
                "    limit 5";
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        SqlNode sqlNode = planner.parse(query);
        EnrichQueryRequest request = EnrichQueryRequest.builder()
                .query(sqlNode)
                .envName(envName)
                .schema(schemas)
                .build();
        assertFalse(conditionsCheckService.isJoinConditionsCorrect(request));
    }

    @Test
    void queryConditionWithTwoJoins() throws SqlParseException {
        String query = "SELECT * FROM dtm_1032.accounts\n" +
                "INNER JOIN dtm_1032.sales on dtm_1032.accounts.id = dtm_1032.sales.id\n" +
                "                                      AND dtm_1032.accounts.account_id = dtm_1032.sales.account_id\n" +
                "                                      AND dtm_1032.accounts.name = dtm_1032.sales.name\n" +
                "INNER JOIN dtm_1033.sales on dtm_1032.accounts.id = dtm_1033.sales.id\n" +
                "                                      AND dtm_1032.accounts.account_id = dtm_1033.sales.account_id\n" +
                "                                      AND dtm_1032.accounts.name = dtm_1033.sales.name\n" +
                "WHERE dtm_1032.accounts.category_id > 5\n" +
                "    limit 5";
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        SqlNode sqlNode = planner.parse(query);
        EnrichQueryRequest request = EnrichQueryRequest.builder()
                .query(sqlNode)
                .envName(envName)
                .schema(schemas)
                .build();
        assertTrue(conditionsCheckService.isJoinConditionsCorrect(request));
    }

    @Test
    void queryConditionWithOneIncorrectJoin() throws SqlParseException {
        String query = "SELECT * FROM dtm_1032.accounts\n" +
                "INNER JOIN dtm_1032.sales on dtm_1032.accounts.id = dtm_1032.sales.id\n" +
                "                                      AND dtm_1032.accounts.account_id = dtm_1032.sales.account_id\n" +
                "                                      AND dtm_1032.accounts.name = dtm_1032.sales.name\n" +
                "INNER JOIN dtm_1033.sales on dtm_1032.accounts.id = dtm_1033.sales.id\n" +
                "                                      AND dtm_1032.accounts.name = dtm_1033.sales.name\n" +
                "WHERE dtm_1032.accounts.category_id > 5\n" +
                "    limit 5";
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        SqlNode sqlNode = planner.parse(query);
        EnrichQueryRequest request = EnrichQueryRequest.builder()
                .query(sqlNode)
                .envName(envName)
                .schema(schemas)
                .build();
        assertFalse(conditionsCheckService.isJoinConditionsCorrect(request));
    }

    @Test
    void queryConditionWithSqlCaseNodeType() throws SqlParseException {
        String query = "SELECT * FROM dtm_1032.products\n" +
                "INNER JOIN dtm_1032.categories on dtm_1032.products.id = CASE WHEN dtm_1032.categories.id = 1 THEN 1 ELSE 2 END\n" +
                "                                      AND dtm_1032.products.product_name = dtm_1032.categories.category_name\n" +
                "WHERE dtm_1032.products.category_id > 5\n" +
                "    limit 5";
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        SqlNode sqlNode = planner.parse(query);
        EnrichQueryRequest request = EnrichQueryRequest.builder()
                .query(sqlNode)
                .envName(envName)
                .schema(schemas)
                .build();
        assertFalse(conditionsCheckService.isJoinConditionsCorrect(request));
    }

    private List<Datamart> createSchema() {
        return Arrays.asList(Datamart.builder()
                        .mnemonic("dtm_1032")
                        .entities(Arrays.asList(Entity.builder()
                                        .name("categories")
                                        .fields(Arrays.asList(EntityField.builder()
                                                        .name("id")
                                                        .shardingOrder(1)
                                                        .build(),
                                                EntityField.builder()
                                                        .name("name")
                                                        .build(),
                                                EntityField.builder()
                                                        .name("category_name")
                                                        .build()))
                                        .build(),
                                Entity.builder()
                                        .name("products")
                                        .fields(Arrays.asList(EntityField.builder()
                                                        .name("id")
                                                        .shardingOrder(1)
                                                        .build(),
                                                EntityField.builder()
                                                        .name("name")
                                                        .build(),
                                                EntityField.builder()
                                                        .name("product_name")
                                                        .build(),
                                                EntityField.builder()
                                                        .name("category_id")
                                                        .build()))
                                        .build(),
                                Entity.builder()
                                        .name("accounts")
                                        .fields(Arrays.asList(EntityField.builder()
                                                        .name("id")
                                                        .shardingOrder(1)
                                                        .build(),
                                                EntityField.builder()
                                                        .name("account_id")
                                                        .shardingOrder(2)
                                                        .build(),
                                                EntityField.builder()
                                                        .name("name")
                                                        .build()))
                                        .build(),
                                Entity.builder()
                                        .name("sales")
                                        .fields(Arrays.asList(EntityField.builder()
                                                        .name("id")
                                                        .shardingOrder(1)
                                                        .build(),
                                                EntityField.builder()
                                                        .name("account_id")
                                                        .shardingOrder(2)
                                                        .build(),
                                                EntityField.builder()
                                                        .name("name")
                                                        .build(),
                                                EntityField.builder()
                                                        .name("category_id")
                                                        .build()))
                                        .build()
                        ))
                        .isDefault(true)
                        .build(),
                Datamart.builder()
                        .mnemonic("dtm_1033")
                        .entities(Arrays.asList(Entity.builder()
                                        .name("categories")
                                        .fields(Arrays.asList(EntityField.builder()
                                                        .name("id")
                                                        .shardingOrder(1)
                                                        .build(),
                                                EntityField.builder()
                                                        .name("name")
                                                        .build(),
                                                EntityField.builder()
                                                        .name("category_name")
                                                        .build()))
                                        .build(),
                                Entity.builder()
                                        .name("products")
                                        .fields(Arrays.asList(EntityField.builder()
                                                        .name("id")
                                                        .shardingOrder(1)
                                                        .build(),
                                                EntityField.builder()
                                                        .name("name")
                                                        .build(),
                                                EntityField.builder()
                                                        .name("product_name")
                                                        .build()))
                                        .build(),
                                Entity.builder()
                                        .name("accounts")
                                        .fields(Arrays.asList(EntityField.builder()
                                                        .name("id")
                                                        .shardingOrder(1)
                                                        .build(),
                                                EntityField.builder()
                                                        .name("account_id")
                                                        .shardingOrder(2)
                                                        .build(),
                                                EntityField.builder()
                                                        .name("name")
                                                        .build()))
                                        .build(),
                                Entity.builder()
                                        .name("sales")
                                        .fields(Arrays.asList(EntityField.builder()
                                                        .name("id")
                                                        .shardingOrder(1)
                                                        .build(),
                                                EntityField.builder()
                                                        .name("account_id")
                                                        .shardingOrder(2)
                                                        .build(),
                                                EntityField.builder()
                                                        .name("name")
                                                        .build()))
                                        .build()
                        ))
                        .isDefault(false)
                        .build());
    }
}