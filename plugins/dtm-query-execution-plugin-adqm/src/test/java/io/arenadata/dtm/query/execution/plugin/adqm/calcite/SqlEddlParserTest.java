package io.arenadata.dtm.query.execution.plugin.adqm.calcite;


import io.arenadata.dtm.calcite.adqm.configuration.AdqmCalciteConfiguration;
import io.arenadata.dtm.query.calcite.core.framework.DtmCalciteFramework;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.CalciteConfiguration;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class SqlEddlParserTest {
    private static final SqlDialect SQL_DIALECT = new SqlDialect(SqlDialect.EMPTY_CONTEXT);
    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final AdqmCalciteConfiguration calciteCoreConfiguration = new AdqmCalciteConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration.configDdlParser(
            calciteCoreConfiguration.eddlParserImplFactory()
    );

    @Test
    void parseSelectWithAliasFinal() throws SqlParseException {
        String expectedResult = "SELECT *\n" +
                "FROM test.pso AS t FINAL";
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        SqlSelect sqlNode = (SqlSelect) planner.parse("select * from test.pso t FINAL");
        assertNotNull(sqlNode);
        assertEquals(expectedResult, sqlNode.toSqlString(SQL_DIALECT).toString());
    }

    @Test
    void parseSelectWithoutAliasFinal() throws SqlParseException {
        String expectedResult = "SELECT *\n" +
                "FROM test.pso FINAL";
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        SqlSelect sqlNode = (SqlSelect) planner.parse("select * from test.pso FINAL");
        assertNotNull(sqlNode);
        assertEquals(expectedResult, sqlNode.toSqlString(SQL_DIALECT).toString());
    }

    @Test
    void parseSelectWithASFinal() throws SqlParseException {
        String expectedResult = "SELECT *\n" +
                "FROM test.pso AS t FINAL";
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        SqlSelect sqlNode = (SqlSelect) planner.parse("select * from test.pso as t FINAL");
        assertNotNull(sqlNode);
        assertEquals(expectedResult, sqlNode.toSqlString(SQL_DIALECT).toString());
    }


}
