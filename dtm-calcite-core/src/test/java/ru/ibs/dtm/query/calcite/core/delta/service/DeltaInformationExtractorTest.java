package ru.ibs.dtm.query.calcite.core.delta.service;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.query.calcite.core.util.DeltaInformationExtractor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@Slf4j
class DeltaInformationExtractorTest {

    public static final String FOR_SYSTEM_TIME = "FOR SYSTEM_TIME";

    @Test
    void extractManySnapshots() throws SqlParseException {
        val sql = "SELECT v.col1 AS c, (SELECT col4 FROM tblc FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59' t3 WHERE tblx.col6 = 0 ) AS r\n" +
                "FROM test.tbl FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' AS t\n" +
                "INNER JOIN (SELECT col4, col5\n" +
                "FROM test2.tblx FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59'\n" +
                "WHERE tblx.col6 = 0) AS v ON t.col3 = v.col4\n" +
                "WHERE EXISTS (SELECT id\n" +
                "FROM (SELECT col4, col5 FROM tblz FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59' WHERE tblz.col6 = 0) AS view) order by v.col1";
        SqlParser.Config config = SqlParser.configBuilder()
                .setParserFactory(SqlParserImpl.FACTORY)
                .setConformance(SqlConformanceEnum.DEFAULT)
                .setLex(Lex.MYSQL)
                .setCaseSensitive(false)
                .setUnquotedCasing(Casing.TO_LOWER)
                .setQuotedCasing(Casing.TO_LOWER)
                .setQuoting(Quoting.DOUBLE_QUOTE)
                .build();
        SqlParser parser = SqlParser.create(sql, config);
        SqlNode sqlNode = parser.parseQuery();
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
        SqlParser.Config config = SqlParser.configBuilder()
                .setParserFactory(SqlParserImpl.FACTORY)
                .setConformance(SqlConformanceEnum.DEFAULT)
                .setLex(Lex.MYSQL)
                .setCaseSensitive(false)
                .setUnquotedCasing(Casing.TO_LOWER)
                .setQuotedCasing(Casing.TO_LOWER)
                .setQuoting(Quoting.DOUBLE_QUOTE)
                .build();
        SqlParser parser = SqlParser.create(sql, config);
        SqlNode sqlNode = parser.parseQuery();
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
        SqlParser.Config config = SqlParser.configBuilder()
                .setParserFactory(SqlParserImpl.FACTORY)
                .setConformance(SqlConformanceEnum.DEFAULT)
                .setLex(Lex.MYSQL)
                .setCaseSensitive(false)
                .setUnquotedCasing(Casing.TO_LOWER)
                .setQuotedCasing(Casing.TO_LOWER)
                .setQuoting(Quoting.DOUBLE_QUOTE)
                .build();
        SqlParser parser = SqlParser.create(sql, config);
        SqlNode sqlNode = parser.parseQuery();
        log.info(sql);
        val deltaInformationResult = DeltaInformationExtractor.extract(sqlNode);
        assertEquals(1, deltaInformationResult.getDeltaInformations().size());
        val sqlWithoutForSystemTime = deltaInformationResult
                .getSqlWithoutSnapshots();
        log.info(sqlWithoutForSystemTime);
        assertFalse(sqlWithoutForSystemTime.contains(FOR_SYSTEM_TIME));
    }
}
