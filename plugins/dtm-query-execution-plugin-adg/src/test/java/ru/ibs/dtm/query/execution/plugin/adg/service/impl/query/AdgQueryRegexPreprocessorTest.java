package ru.ibs.dtm.query.execution.plugin.adg.service.impl.query;

import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.plugin.adg.dto.RegexPreprocessorResult;

import java.util.Map;
import java.util.regex.Matcher;

import static org.junit.jupiter.api.Assertions.*;

class AdgQueryRegexPreprocessorTest {
  @Test
  void extractGroups_When1Systime0Join() {
    String before = "sElEcT * fRoM Table1 FOR SYSTEM_TIME AS OF TIMESTAMP '1999-01-08 04:05:06' where <условие> ; ";
    Matcher matcher = AdgQueryRegexPreprocessor.SELECT_FOR_SYSTEM_TIME.matcher(before);
    assertTrue(matcher.matches());
    assertEquals(7, matcher.groupCount());
    assertEquals("*", matcher.group(1));
    assertEquals("Table1", matcher.group(2));
    assertEquals(" FOR SYSTEM_TIME AS OF TIMESTAMP '1999-01-08 04:05:06'", matcher.group(3));
    assertEquals("TIMESTAMP ", matcher.group(4));
    assertEquals("1999-01-08 04:05:06", matcher.group(5));
    assertEquals("<условие> ; ", matcher.group(7));
    final String after = AdgQueryRegexPreprocessor.cutGroup(before, matcher, 3);
    assertEquals("sElEcT * fRoM Table1 where <условие> ; ", after);
  }

  @Test
  void extractGroups_When1Systime1Join() {
    String before = "sElEcT * fRoM Table1 FOR SYSTEM_TIME AS OF TIMESTAMP '1999-01-08 04:05:06' join X on Y where <условие>;";
    Matcher matcher = AdgQueryRegexPreprocessor.SELECT_FOR_SYSTEM_TIME.matcher(before);
    assertTrue(matcher.matches());
    assertEquals(7, matcher.groupCount());
    assertEquals("*", matcher.group(1));
    assertEquals("Table1", matcher.group(2));
    assertEquals(" FOR SYSTEM_TIME AS OF TIMESTAMP '1999-01-08 04:05:06'", matcher.group(3));
    assertEquals("TIMESTAMP ", matcher.group(4));
    assertEquals("1999-01-08 04:05:06", matcher.group(5));
    assertEquals("<условие>;", matcher.group(7));
    final String after = AdgQueryRegexPreprocessor.cutGroup(before, matcher, 3);
    assertEquals("sElEcT * fRoM Table1 join X on Y where <условие>;", after);
  }

  @Test
  void extractGroups_WhenNoTimestampNoWhere() {
    String before = "SELECT * FROM Table1 FOR SYSTEM_TIME AS OF '1999-01-08 04:05:06'";
    final Matcher matcher = AdgQueryRegexPreprocessor.SELECT_FOR_SYSTEM_TIME.matcher(before);
    assertTrue(matcher.matches());
    assertEquals(7, matcher.groupCount());
    assertEquals("1999-01-08 04:05:06", matcher.group(5));
    final String after = AdgQueryRegexPreprocessor.cutGroup(before, matcher, 3);
    assertEquals("SELECT * FROM Table1", after);
  }

  @Test
  void extractGroups_WhenNoSystime() {
    String before = "SELECT * FROM Table1 WHERE <условие>";
    final Matcher matcher = AdgQueryRegexPreprocessor.SELECT_FOR_SYSTEM_TIME.matcher(before);
    assertTrue(matcher.matches());
    assertEquals(7, matcher.groupCount());
    assertEquals("*", matcher.group(1));
    assertEquals("Table1", matcher.group(2));
    assertNull(matcher.group(5));
    assertEquals("<условие>", matcher.group(7));
    final String after = AdgQueryRegexPreprocessor.cutGroup(before, matcher, 3);
    assertEquals("SELECT * FROM Table1 WHERE <условие>", after);
  }

  @Test
  void extractGroupsWhenContainsNewLine() {
    final QueryRequest queryRequest = new QueryRequest();
    queryRequest.setSql("SELECT * FROM Table1 \r\nFOR SYSTEM_TIME AS OF '1999-01-08 04:05:06'");
    final RegexPreprocessorResult result = new RegexPreprocessorResult(queryRequest);
    new AdgQueryRegexPreprocessor().applyAllModifiers(result);
    assertTrue(result.isSqlModified());
    assertEquals("SELECT * FROM Table1", result.getModifiedSql());
    assertEquals("1999-01-08 04:05:06", result.getSystemTimesForTables().get("Table1"));
  }

  @Test
  void applyAllModifiers() {
    final QueryRequest queryRequest = new QueryRequest();
    queryRequest.setSql("SELECT * FROM Table1 FOR SYSTEM_TIME AS OF '1999-01-08 04:05:06'");
    final RegexPreprocessorResult result = new RegexPreprocessorResult(queryRequest);
    new AdgQueryRegexPreprocessor().applyAllModifiers(result);
    assertTrue(result.isSqlModified());
    assertEquals("SELECT * FROM Table1", result.getModifiedSql());
    assertEquals("1999-01-08 04:05:06", result.getSystemTimesForTables().get("Table1"));
  }

  @Test
  void modifyForSystemTime_WhenForSystemTimeIsAbsent() {
    final QueryRequest queryRequest = new QueryRequest();
    queryRequest.setSql("SELECT * FROM Table1");
    final RegexPreprocessorResult result = new RegexPreprocessorResult(queryRequest);

    final Matcher matcher = AdgQueryRegexPreprocessor.SELECT_FOR_SYSTEM_TIME.matcher(queryRequest.getSql());
    assertTrue(matcher.matches());
    new AdgQueryRegexPreprocessor().modifyForSystemTime(matcher, result);

    final String systemTimeAsOf = result.getSystemTimesForTables().get("Table1");
    assertValidDateTime(systemTimeAsOf);
  }

  @Test
  void modifyForSystemTime_When2TemporalTable() {
    final String before = "SELECT Col1, Col2 " +
      "FROM tbl1 FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' " +
      "JOIN tbl2 FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59' " +
      "ON tbl1.Col3 = tbl2.Col4 " +
      "WHERE tbl1.Col5 = 1;";
    final Matcher matcher = AdgQueryRegexPreprocessor.SELECT_FOR_SYSTEM_TIME.matcher(before);
    assertTrue(matcher.matches());

    final QueryRequest queryRequest = new QueryRequest();
    queryRequest.setSql(before);
    final RegexPreprocessorResult result = new RegexPreprocessorResult(queryRequest);

    new AdgQueryRegexPreprocessor().applyAllModifiers(result);

    final Map<String, String> systemTimesForTables = result.getSystemTimesForTables();
    assertEquals(2, systemTimesForTables.size());
    assertEquals("2019-12-23 15:15:14", systemTimesForTables.get("tbl1"));
    assertEquals("2018-07-29 23:59:59", systemTimesForTables.get("tbl2"));
    assertEquals("SELECT Col1, Col2 FROM tbl1 JOIN tbl2 ON tbl1.Col3 = tbl2.Col4 WHERE tbl1.Col5 = 1;",
      result.getModifiedSql());
  }

  @Test
  void modifyForSystemTime_When1TemporalTableThenNoSystimeTable() {
    final String before = "SELECT Col1, Col2 " +
      "FROM tbl1 FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' " +
      "JOIN tbl2 " +
      "ON tbl1.Col3 = tbl2.Col4 " +
      "WHERE tbl1.Col5 = 1";
    final Matcher matcher = AdgQueryRegexPreprocessor.SELECT_FOR_SYSTEM_TIME.matcher(before);
    assertTrue(matcher.matches());

    final QueryRequest queryRequest = new QueryRequest();
    queryRequest.setSql(before);
    final RegexPreprocessorResult result = new RegexPreprocessorResult(queryRequest);

    new AdgQueryRegexPreprocessor().applyAllModifiers(result);

    final Map<String, String> systemTimesForTables = result.getSystemTimesForTables();
    assertEquals(2, systemTimesForTables.size());
    assertEquals("2019-12-23 15:15:14", systemTimesForTables.get("tbl1"));
    assertValidDateTime(systemTimesForTables.get("tbl2"));
    assertEquals("SELECT Col1, Col2 FROM tbl1 JOIN tbl2 ON tbl1.Col3 = tbl2.Col4 WHERE tbl1.Col5 = 1",
      result.getModifiedSql());
    assertEquals(result.getModifiedSql(), result.getActualQueryRequest().getSql());
  }

  @Test
  void extractGroups_WhenTableInQuotes() {
    String before = "SELECT * FROM \"Table1\" FOR SYSTEM_TIME AS OF '1999-01-08 04:05:06'";
    final Matcher matcher = AdgQueryRegexPreprocessor.SELECT_FOR_SYSTEM_TIME.matcher(before);
    assertTrue(matcher.matches());
    assertEquals(7, matcher.groupCount());
    assertEquals("1999-01-08 04:05:06", matcher.group(5));
    final String after = AdgQueryRegexPreprocessor.cutGroup(before, matcher, 3);
    assertEquals("SELECT * FROM \"Table1\"", after);
  }

  private void assertValidDateTime(String datetime) {
    assertNotNull(datetime);
    assertEquals(19, datetime.length());
  }
}
