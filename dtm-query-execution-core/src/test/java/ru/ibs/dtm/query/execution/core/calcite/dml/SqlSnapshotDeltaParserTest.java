package ru.ibs.dtm.query.execution.core.calcite.dml;


import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.delta.DeltaInterval;
import ru.ibs.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.ibs.dtm.query.calcite.core.extension.snapshot.SqlSnapshot;
import ru.ibs.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;

import static org.junit.jupiter.api.Assertions.*;

public class SqlSnapshotDeltaParserTest {
    private static final SqlDialect SQL_DIALECT = new SqlDialect(SqlDialect.EMPTY_CONTEXT);
    private CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(
            calciteCoreConfiguration.eddlParserImplFactory()
    );

    @Test
    void parseSnapshotWithDeltaDateTime() throws SqlParseException {
        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = Frameworks.getPlanner(frameworkConfig);

        SqlNode sqlNode = planner.parse("select * from test.pso FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14'");
        assertNotNull(sqlNode);
        assertNull(((SqlSnapshot) ((SqlSelect) sqlNode).getFrom()).getLatestUncommitedDelta());
        assertEquals(((SqlSnapshot) ((SqlSelect) sqlNode).getFrom()).getPeriod().toSqlString(SQL_DIALECT).toString(), "'2019-12-23 15:15:14'");
        assertEquals(sqlNode.toSqlString(SQL_DIALECT).toString(), "SELECT *\nFROM test.pso FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14'");
    }

    @Test
    void parseSnapshotWithLatestUncommitedDelta() throws SqlParseException {
        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = Frameworks.getPlanner(frameworkConfig);

        SqlNode sqlNode = planner.parse("select * from test.pso FOR SYSTEM_TIME AS OF LATEST_UNCOMMITED_DELTA");
        assertTrue(((SqlSnapshot) ((SqlSelect) sqlNode).getFrom()).getLatestUncommitedDelta());
        assertEquals(sqlNode.toSqlString(SQL_DIALECT).toString(), "SELECT *\nFROM test.pso FOR SYSTEM_TIME AS OF LATEST_UNCOMMITED_DELTA");
    }

    @Test
    void parseSnapshotWithStartedInInterval() throws SqlParseException {
        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = Frameworks.getPlanner(frameworkConfig);
        DeltaInterval startedInterval = new DeltaInterval(1L, 3L);
        SqlNode sqlNode = planner.parse("select * from test.pso FOR SYSTEM_TIME STARTED IN (1,3)");
        assertNull(((SqlSnapshot) ((SqlSelect) sqlNode).getFrom()).getFinishedInterval());
        assertNull(((SqlSnapshot) ((SqlSelect) sqlNode).getFrom()).getDeltaDateTime());
        assertNull(((SqlSnapshot) ((SqlSelect) sqlNode).getFrom()).getLatestUncommitedDelta());
        assertEquals(startedInterval, ((SqlSnapshot) ((SqlSelect) sqlNode).getFrom()).getStartedInterval());
        assertEquals(sqlNode.toSqlString(SQL_DIALECT).toString(), "SELECT *\nFROM test.pso FOR SYSTEM_TIME STARTED IN (1,3)");
    }

    @Test
    void parseSnapshotWithIncorrectStartedInInterval() {
        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = Frameworks.getPlanner(frameworkConfig);
        Assertions.assertThrows(SqlParseException.class, () -> {
            SqlNode sqlNode = planner.parse("select * from test.pso FOR SYSTEM_TIME STARTED IN (5,3)");
        });
        Assertions.assertThrows(SqlParseException.class, () -> {
            SqlNode sqlNode = planner.parse("select * from test.pso FOR SYSTEM_TIME STARTED IN (5)");
        });
        Assertions.assertThrows(SqlParseException.class, () -> {
            SqlNode sqlNode = planner.parse("select * from test.pso FOR SYSTEM_TIME STARTED IN ('1',5)");
        });
        Assertions.assertThrows(SqlParseException.class, () -> {
            SqlNode sqlNode = planner.parse("select * from test.pso FOR SYSTEM_TIME STARTED IN (1,5,4)");
        });
    }

    @Test
    void parseSnapshotWithFinishedInInterval() throws SqlParseException {
        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = Frameworks.getPlanner(frameworkConfig);
        DeltaInterval finishedInterval = new DeltaInterval(1L, 3L);
        SqlNode sqlNode = planner.parse("select * from test.pso FOR SYSTEM_TIME FINISHED IN (1,3)");
        assertNull(((SqlSnapshot) ((SqlSelect) sqlNode).getFrom()).getStartedInterval());
        assertNull(((SqlSnapshot) ((SqlSelect) sqlNode).getFrom()).getDeltaDateTime());
        assertNull(((SqlSnapshot) ((SqlSelect) sqlNode).getFrom()).getLatestUncommitedDelta());
        assertEquals(finishedInterval, ((SqlSnapshot) ((SqlSelect) sqlNode).getFrom()).getFinishedInterval());
        assertEquals(sqlNode.toSqlString(SQL_DIALECT).toString(), "SELECT *\nFROM test.pso FOR SYSTEM_TIME FINISHED IN (1,3)");
    }

    @Test
    void parseSnapshotWithIncorrectFinishedInInterval() {
        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = Frameworks.getPlanner(frameworkConfig);
        Assertions.assertThrows(SqlParseException.class, () -> {
            SqlNode sqlNode = planner.parse("select * from test.pso FOR SYSTEM_TIME FINISHED IN (5,3)");
        });
        Assertions.assertThrows(SqlParseException.class, () -> {
            SqlNode sqlNode = planner.parse("select * from test.pso FOR SYSTEM_TIME FINISHED IN (5)");
        });
        Assertions.assertThrows(SqlParseException.class, () -> {
            SqlNode sqlNode = planner.parse("select * from test.pso FOR SYSTEM_TIME FINISHED IN ('1',5)");
        });
        Assertions.assertThrows(SqlParseException.class, () -> {
            SqlNode sqlNode = planner.parse("select * from test.pso FOR SYSTEM_TIME FINISHED IN (1,5,4)");
        });
    }

    @Test
    void parseSnapshotWithDeltaNum() throws SqlParseException {
        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = Frameworks.getPlanner(frameworkConfig);

        SqlNode sqlNode = planner.parse("select * from test.pso FOR SYSTEM_TIME AS OF DELTA_NUM 1");
        assertNotNull(sqlNode);
        assertNull(((SqlSnapshot) ((SqlSelect) sqlNode).getFrom()).getLatestUncommitedDelta());
        assertEquals(((SqlSnapshot) ((SqlSelect) sqlNode).getFrom()).getDeltaNum(), 1L);
        assertEquals(sqlNode.toSqlString(SQL_DIALECT).toString(), "SELECT *\nFROM test.pso FOR SYSTEM_TIME AS OF DELTA_NUM 1");
    }

    @Test
    void parseSnaapshotWithIncorrectDeltaNum() {
        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = Frameworks.getPlanner(frameworkConfig);
        Assertions.assertThrows(SqlParseException.class, () -> {
            SqlNode sqlNode = planner.parse("select * from test.pso FOR SYSTEM_TIME AS OF DELTA_NUM '1'");
        });
    }
}