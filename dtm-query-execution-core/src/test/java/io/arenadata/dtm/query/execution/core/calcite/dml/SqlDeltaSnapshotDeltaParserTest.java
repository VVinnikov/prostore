package io.arenadata.dtm.query.execution.core.calcite.dml;


import io.arenadata.dtm.common.delta.SelectOnInterval;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.extension.dml.LimitableSqlOrderBy;
import io.arenadata.dtm.query.calcite.core.extension.dml.SqlSelectExt;
import io.arenadata.dtm.query.calcite.core.extension.snapshot.SqlDeltaSnapshot;
import io.arenadata.dtm.query.calcite.core.framework.DtmCalciteFramework;
import io.arenadata.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class SqlDeltaSnapshotDeltaParserTest {
    private static final SqlDialect SQL_DIALECT = new SqlDialect(SqlDialect.EMPTY_CONTEXT);
    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(
            calciteCoreConfiguration.eddlParserImplFactory()
    );

    @Test
    void parseSnapshotWithDeltaDateTime() throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        SqlNode sqlNode = planner.parse("select * from test.pso FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14'");
        assertNotNull(sqlNode);
        assertFalse(((SqlDeltaSnapshot) ((SqlSelect) sqlNode).getFrom()).getLatestUncommittedDelta());
        assertEquals(((SqlDeltaSnapshot) ((SqlSelect) sqlNode).getFrom()).getPeriod().toSqlString(SQL_DIALECT).toString(), "'2019-12-23 15:15:14'");
        assertEquals(sqlNode.toSqlString(SQL_DIALECT).toString(), "SELECT *\nFROM test.pso FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14'");
    }

    @Test
    void parseSnapshotWithLatestUncommittedDelta() throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        SqlNode sqlNode = planner.parse("select * from test.pso FOR SYSTEM_TIME AS OF LATEST_UNCOMMITTED_DELTA");
        assertTrue(((SqlDeltaSnapshot) ((SqlSelect) sqlNode).getFrom()).getLatestUncommittedDelta());
        assertEquals(sqlNode.toSqlString(SQL_DIALECT).toString(), "SELECT *\nFROM test.pso FOR SYSTEM_TIME AS OF LATEST_UNCOMMITTED_DELTA");
    }

    @Test
    void parseSnapshotWithStartedInInterval() throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        SelectOnInterval startedInterval = new SelectOnInterval(1L, 3L);
        SqlNode sqlNode = planner.parse("select * from test.pso FOR SYSTEM_TIME STARTED IN (1,3)");
        assertNull(((SqlDeltaSnapshot) ((SqlSelect) sqlNode).getFrom()).getFinishedInterval());
        assertNull(((SqlDeltaSnapshot) ((SqlSelect) sqlNode).getFrom()).getDeltaDateTime());
        assertFalse(((SqlDeltaSnapshot) ((SqlSelect) sqlNode).getFrom()).getLatestUncommittedDelta());
        assertEquals(startedInterval, ((SqlDeltaSnapshot) ((SqlSelect) sqlNode).getFrom()).getStartedInterval());
        assertEquals(sqlNode.toSqlString(SQL_DIALECT).toString(), "SELECT *\nFROM test.pso FOR SYSTEM_TIME STARTED IN (1,3)");
    }

    @Test
    void parseSnapshotWithIncorrectStartedInInterval() {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
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
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        SelectOnInterval finishedInterval = new SelectOnInterval(1L, 3L);
        SqlNode sqlNode = planner.parse("select * from test.pso FOR SYSTEM_TIME FINISHED IN (1,3)");
        assertNull(((SqlDeltaSnapshot) ((SqlSelect) sqlNode).getFrom()).getStartedInterval());
        assertNull(((SqlDeltaSnapshot) ((SqlSelect) sqlNode).getFrom()).getDeltaDateTime());
        assertFalse(((SqlDeltaSnapshot) ((SqlSelect) sqlNode).getFrom()).getLatestUncommittedDelta());
        assertEquals(finishedInterval, ((SqlDeltaSnapshot) ((SqlSelect) sqlNode).getFrom()).getFinishedInterval());
        assertEquals(sqlNode.toSqlString(SQL_DIALECT).toString(), "SELECT *\nFROM test.pso FOR SYSTEM_TIME FINISHED IN (1,3)");
    }

    @Test
    void parseSnapshotWithIncorrectFinishedInInterval() {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
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
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        SqlNode sqlNode = planner.parse("select * from test.pso FOR SYSTEM_TIME AS OF DELTA_NUM 1");
        assertNotNull(sqlNode);
        assertFalse(((SqlDeltaSnapshot) ((SqlSelect) sqlNode).getFrom()).getLatestUncommittedDelta());
        assertEquals(((SqlDeltaSnapshot) ((SqlSelect) sqlNode).getFrom()).getDeltaNum(), 1L);
        assertEquals(sqlNode.toSqlString(SQL_DIALECT).toString(), "SELECT *\nFROM test.pso FOR SYSTEM_TIME AS OF DELTA_NUM 1");
    }

    @Test
    void parseSnaapshotWithIncorrectDeltaNum() {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        Assertions.assertThrows(SqlParseException.class, () -> {
            SqlNode sqlNode = planner.parse("select * from test.pso FOR SYSTEM_TIME AS OF DELTA_NUM '1'");
        });
    }

    @Test
    void parseSelectWithCase() throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        SqlNode sqlNode = planner.parse("select case when id > 1 then 'ok' else 'not ok' end " +
                " from test.pso FOR SYSTEM_TIME AS OF DELTA_NUM 1");
        assertNotNull(sqlNode);
    }

    @Test
    void parseSelectWithDatasourceType() throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        SqlSelectExt sqlNode = (SqlSelectExt) planner.parse("select * from test.pso DATASOURCE_TYPE='adg'");
        assertEquals("adg", sqlNode.getDatasourceType().getNlsString().getValue());
    }

    @Test
    void parseSelectWithDatasourceTypeAndConditions() throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        SqlSelectExt sqlNode = (SqlSelectExt) planner.parse("select * from test.pso where id = 1 DATASOURCE_TYPE='adg'");
        assertEquals("adg", sqlNode.getDatasourceType().getNlsString().getValue());
    }

    @Test
    void parseSelectWithDatasourceTypeAndOrderBy() throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        LimitableSqlOrderBy sqlNode = (LimitableSqlOrderBy) planner.parse("select * from test.pso order by id DATASOURCE_TYPE='adg'");
        assertEquals("adg", sqlNode.getDatasourceType().getNlsString().getValue());
    }

    @Test
    void parseSelectWithDatasourceTypeAndLimit() throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        LimitableSqlOrderBy sqlNode = (LimitableSqlOrderBy) planner.parse("select * from test.pso limit 10 DATASOURCE_TYPE='adg'");
        assertEquals("adg", sqlNode.getDatasourceType().getNlsString().getValue());
    }

    @Test
    void parseSelectWithDatasourceTypeWithoutFrom() throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        Assertions.assertThrows(SqlParseException.class, () -> {
            planner.parse("select 1 DATASOURCE_TYPE='adb");
        });
    }
}
