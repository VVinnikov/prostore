package io.arenadata.dtm.query.execution.plugin.adb.service.impl.query;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.plugin.adb.dto.RegexPreprocessorResult;
import org.junit.jupiter.api.Test;

import java.util.regex.Matcher;

import static org.junit.jupiter.api.Assertions.*;

class QueryRegexPreprocessorTest {
  @Test
  void extractGroups() {
    String before = "select * from test_datamart.pso FOR SYSTEM_TIME AS OF TIMESTAMP '1999-01-08 04:05:06' ;";
    final Matcher matcher = QueryRegexPreprocessor.SELECT_FOR_SYSTEM_TIME.matcher(before);

    assertTrue(matcher.matches());
    assertEquals(7, matcher.groupCount());
    assertEquals("*", matcher.group(1));
    assertEquals("test_datamart.pso", matcher.group(2));
    assertEquals(" FOR SYSTEM_TIME AS OF TIMESTAMP '1999-01-08 04:05:06'", matcher.group(3));
    final String dateTime = matcher.group(5);
    assertEquals("1999-01-08 04:05:06", dateTime);
    assertNull(matcher.group(7));
    final String after = QueryRegexPreprocessor.cutGroup(before, matcher, 3);
    assertEquals("select * from test_datamart.pso ;", after);
  }

  @Test
  void applyAllModifiers() {
    final QueryRequest queryRequest = new QueryRequest();
    queryRequest.setSql("select * from test_datamart.pso FOR SYSTEM_TIME AS OF TIMESTAMP '1999-01-08 04:05:06';");
    final RegexPreprocessorResult result = new RegexPreprocessorResult(queryRequest);
    new QueryRegexPreprocessor().applyAllModifiers(result);
    assertTrue(result.isSqlModified());
    assertEquals("select * from test_datamart.pso;", result.getModifiedSql());
    assertEquals("1999-01-08 04:05:06", result.getSystemTimeAsOf());
  }

  @Test
  void extractWithQuotes() {
    String before = "select * from \"test_datamart\".\"pso\"";
    final Matcher matcher = QueryRegexPreprocessor.SELECT_FOR_SYSTEM_TIME.matcher(before);

    assertTrue(matcher.matches());
    assertEquals("\"test_datamart\".\"pso\"", matcher.group(2));
  }

  @Test
  void extractGroupsWhenContainsNewLine() {
    final QueryRequest queryRequest = new QueryRequest();
    queryRequest.setSql("select * from test_datamart.pso \r\nFOR SYSTEM_TIME AS OF TIMESTAMP '1999-01-08 04:05:06';");
    final RegexPreprocessorResult result = new RegexPreprocessorResult(queryRequest);
    new QueryRegexPreprocessor().applyAllModifiers(result);
    assertTrue(result.isSqlModified());
    assertEquals("select * from test_datamart.pso;", result.getModifiedSql());
    assertEquals("1999-01-08 04:05:06", result.getSystemTimeAsOf());
  }
}
