package ru.ibs.dtm.query.calcite.core.delta.service;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.delta.DeltaInformationResult;
import ru.ibs.dtm.common.delta.DeltaType;
import ru.ibs.dtm.common.delta.SelectOnInterval;
import ru.ibs.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.ibs.dtm.query.calcite.core.framework.DtmCalciteFramework;
import ru.ibs.dtm.query.calcite.core.util.DeltaInformationExtractor;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
class DeltaInformationExtractorTest {

    private static final String FOR_SYSTEM_TIME = "FOR SYSTEM_TIME";

    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private Planner planner;

    @BeforeEach
    void setUp() {
        SqlParser.Config parserConfig = SqlParser.configBuilder()
                .setParserFactory(calciteCoreConfiguration.eddlParserImplFactory())
                .setConformance(SqlConformanceEnum.DEFAULT)
                .setLex(Lex.MYSQL)
                .setCaseSensitive(false)
                .setUnquotedCasing(Casing.TO_LOWER)
                .setQuotedCasing(Casing.TO_LOWER)
                .setQuoting(Quoting.DOUBLE_QUOTE)
                .build();
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        planner = DtmCalciteFramework.getPlanner(frameworkConfig);
    }

    @Test
    void extractManySnapshots() throws SqlParseException {
        val sql = "SELECT v.col1 AS c, (SELECT col4 FROM tblc FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59' t3 WHERE tblx.col6 = 0 ) AS r\n" +
                "FROM test.tbl FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' AS t\n" +
                "INNER JOIN (SELECT col4, col5\n" +
                "FROM test2.tblx FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59'\n" +
                "WHERE tblx.col6 = 0) AS v ON t.col3 = v.col4\n" +
                "WHERE EXISTS (SELECT id\n" +
                "FROM (SELECT col4, col5 FROM tblz FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59' WHERE tblz.col6 = 0) AS view) order by v.col1";
        SqlNode sqlNode = planner.parse(sql);
        val deltaInformationResult = DeltaInformationExtractor.extract(sqlNode);
        log.info(deltaInformationResult.toString());
        assertEquals(4, deltaInformationResult.getDeltaInformations().size());
        val sqlWithoutForSystemTime = deltaInformationResult
                .getSqlWithoutSnapshots();
        log.info(sqlWithoutForSystemTime);
        assertFalse(sqlWithoutForSystemTime.contains(FOR_SYSTEM_TIME));
    }

    @Test
    void extractOneSnapshot() throws SqlParseException {
        val sql = "SELECT v.col1 AS c\n" +
                "FROM test.tbl FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' v";
        SqlNode sqlNode = planner.parse(sql);
        log.info(sql);
        val deltaInformationResult = DeltaInformationExtractor.extract(sqlNode);
        assertEquals(1, deltaInformationResult.getDeltaInformations().size());
        val sqlWithoutForSystemTime = deltaInformationResult
                .getSqlWithoutSnapshots();
        log.info(sqlWithoutForSystemTime);
        assertFalse(sqlWithoutForSystemTime.contains(FOR_SYSTEM_TIME));
    }


    @Test
    void extractWithoutSnapshot() throws SqlParseException {
        val sql = "SELECT v.col1 AS c FROM (SELECT v.col1 AS c FROM tbl as z) v";
        SqlNode sqlNode = planner.parse(sql);
        log.info(sql);
        val deltaInformationResult = DeltaInformationExtractor.extract(sqlNode);
        assertEquals(1, deltaInformationResult.getDeltaInformations().size());
        val sqlWithoutForSystemTime = deltaInformationResult
                .getSqlWithoutSnapshots();
        log.info(sqlWithoutForSystemTime);
        assertFalse(sqlWithoutForSystemTime.contains(FOR_SYSTEM_TIME));
    }

    @Test
    void extractWithLatestUncommitedDeltaSnapshot() throws SqlParseException {
        val sql = "SELECT v.col1 AS c, (SELECT col4 FROM tblc FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59' t3 WHERE tblx.col6 = 0 ) AS r\n" +
                "FROM test.tbl FOR SYSTEM_TIME AS OF LATEST_UNCOMMITED_DELTA AS t\n" +
                "INNER JOIN (SELECT col4, col5\n" +
                "FROM test2.tblx FOR SYSTEM_TIME AS OF LATEST_UNCOMMITED_DELTA\n" +
                "WHERE tblx.col6 = 0) AS v ON t.col3 = v.col4\n" +
                "WHERE EXISTS (SELECT id\n" +
                "FROM (SELECT col4, col5 FROM tblz FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59' WHERE tblz.col6 = 0) AS view) order by v.col1";
        SqlNode sqlNode = planner.parse(sql);
        log.info(sql);
        DeltaInformationResult deltaInformationResult = DeltaInformationExtractor.extract(sqlNode);
        assertEquals(4, deltaInformationResult.getDeltaInformations().size());
        assertTrue(deltaInformationResult.getDeltaInformations().get(1).isLatestUncommitedDelta());
        assertTrue(deltaInformationResult.getDeltaInformations().get(2).isLatestUncommitedDelta());
        assertNull(deltaInformationResult.getDeltaInformations().get(1).getDeltaTimestamp());
        assertNull(deltaInformationResult.getDeltaInformations().get(2).getDeltaTimestamp());

        val sqlWithoutForSystemTime = deltaInformationResult.getSqlWithoutSnapshots();
        log.info(sqlWithoutForSystemTime);
        assertFalse(sqlWithoutForSystemTime.contains(FOR_SYSTEM_TIME));
    }

    @Test
    void extractWithStartedDeltaInterval() throws SqlParseException {
        val sql = "SELECT v.col1 AS c, (SELECT col4 FROM tblc FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59' t3 WHERE tblx.col6 = 0 ) AS r\n" +
                "FROM test.tbl FOR SYSTEM_TIME AS OF LATEST_UNCOMMITED_DELTA AS t\n" +
                "INNER JOIN (SELECT col4, col5\n" +
                "FROM test2.tblx FOR SYSTEM_TIME STARTED IN (1, 2)\n" +
                "WHERE tblx.col6 = 0) AS v ON t.col3 = v.col4\n" +
                "INNER JOIN (SELECT col4, col5\n" +
                "FROM test2.tbly FOR SYSTEM_TIME AS OF DELTA_NUM 4444\n" +
                "WHERE tbly.col6 = 0) AS vv ON t.col3 = vv.col4\n" +
                "WHERE EXISTS (SELECT id\n" +
                "FROM (SELECT col4, col5 FROM tblz FOR SYSTEM_TIME FINISHED IN (3,4) WHERE tblz.col6 = 0) AS view) order by v.col1";
        SqlNode sqlNode = planner.parse(sql);
        log.info(sql);
        DeltaInformationResult deltaInformationResult = DeltaInformationExtractor.extract(sqlNode);
        assertEquals(5, deltaInformationResult.getDeltaInformations().size());

        assertNotNull(deltaInformationResult.getDeltaInformations().get(0).getDeltaTimestamp());
        assertTrue(deltaInformationResult.getDeltaInformations().get(1).isLatestUncommitedDelta());
        assertEquals(new SelectOnInterval(1L,2L), deltaInformationResult.getDeltaInformations().get(2).getSelectOnInterval());
        assertEquals(DeltaType.STARTED_IN, deltaInformationResult.getDeltaInformations().get(2).getType());
        assertEquals(4444, deltaInformationResult.getDeltaInformations().get(3).getSelectOnNum());
        assertEquals(new SelectOnInterval(3L,4L), deltaInformationResult.getDeltaInformations().get(4).getSelectOnInterval());
        assertEquals(DeltaType.FINISHED_IN, deltaInformationResult.getDeltaInformations().get(4).getType());

        val sqlWithoutForSystemTime = deltaInformationResult.getSqlWithoutSnapshots();
        log.info(sqlWithoutForSystemTime);
        assertFalse(sqlWithoutForSystemTime.contains(FOR_SYSTEM_TIME));
    }
}
