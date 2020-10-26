package io.arenadata.dtm.query.execution.plugin.adg.service.impl.query;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.plugin.adg.dto.RegexPreprocessorResult;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.temporal.ChronoField.*;

/**
 * Парсер для извлечения systemtime для каждой таблицы из SQL-запроса
 */
@Service
public class AdgQueryRegexPreprocessor {
  private static final Logger LOGGER = LoggerFactory.getLogger(AdgQueryRegexPreprocessor.class);

  private static final DateTimeFormatter LOCAL_DATE_TIME = new DateTimeFormatterBuilder()
    .parseCaseInsensitive()
    .append(ISO_LOCAL_DATE)
    .appendLiteral(' ')
    .appendValue(HOUR_OF_DAY, 2)
    .appendLiteral(':')
    .appendValue(MINUTE_OF_HOUR, 2)
    .optionalStart()
    .appendLiteral(':')
    .appendValue(SECOND_OF_MINUTE, 2)
    .toFormatter();

  private static final String POSSIBLE_SYSTEM_TIME =
    "(\\s+FOR\\s+SYSTEM_TIME\\s+AS\\s+OF\\s+(TIMESTAMP\\s+)?'([0-9\\-]+\\s+[0-9:]+)')?";

  static final Pattern SELECT_FOR_SYSTEM_TIME = Pattern.compile(
    "\\s*select\\s+(.*)\\s+from\\s+([A-z.0-9\"]+)" +
      POSSIBLE_SYSTEM_TIME +
      ".*?(\\s+where\\s(.*))?",
    Pattern.CASE_INSENSITIVE
  );
  private static final Pattern TABLE_AND_SYSTEM_TIME = Pattern.compile(
    "\\s+(FROM|JOIN)\\s+([A-z.0-9\"]+)" +
      POSSIBLE_SYSTEM_TIME,
    Pattern.CASE_INSENSITIVE
  );

  private PatternModifierPair[] patternModifierPairs;

  public AdgQueryRegexPreprocessor() {
    patternModifierPairs = new PatternModifierPair[]{
      new PatternModifierPair(SELECT_FOR_SYSTEM_TIME, this::modifyForSystemTime)
    };
  }

  public void process(QueryRequest queryRequest, Handler<AsyncResult<RegexPreprocessorResult>> handler) {
    final RegexPreprocessorResult result = new RegexPreprocessorResult(queryRequest);
    applyAllModifiers(result);
    handler.handle(Future.succeededFuture(result));
  }

  void applyAllModifiers(RegexPreprocessorResult result) {
    for (PatternModifierPair patternModifierPair : patternModifierPairs) {
      final Matcher matcher = patternModifierPair.regexPattern.matcher(result.getModifiedSql());
      if (matcher.matches()) {
        LOGGER.debug("Regex worked: {}", patternModifierPair.regexPattern);
        patternModifierPair.sqlModifier.modify(matcher, result);
        LOGGER.debug("Changed SQL: {}", result.getModifiedSql());
      }
    }
  }

  void modifyForSystemTime(Matcher matcher, RegexPreprocessorResult result) {
    final String originalSql = result.getOriginalQueryRequest().getSql();
    final StringBuilder sqlWithoutSystemTime = new StringBuilder(originalSql.length());
    final Matcher tableSysTimeMatcher = TABLE_AND_SYSTEM_TIME.matcher(originalSql);
    int remainIndex = 0;
    Map<String, String> systemTimesForTables = new HashMap<>();
    while (tableSysTimeMatcher.find()) {
      final String table = tableSysTimeMatcher.group(2);
      final String dateTime = tableSysTimeMatcher.group(5);
      systemTimesForTables.put(table, dateTime != null ? dateTime : currentDateTime());
      final int startPossibleSystime = tableSysTimeMatcher.start(3);
      if (startPossibleSystime == -1) {
        final int regexEnd = tableSysTimeMatcher.end();
        sqlWithoutSystemTime.append(originalSql, remainIndex, regexEnd);
        remainIndex = regexEnd;
      } else {
        sqlWithoutSystemTime.append(originalSql, remainIndex, startPossibleSystime);
        remainIndex = tableSysTimeMatcher.end(3);
      }
    }
    sqlWithoutSystemTime.append(originalSql, remainIndex, originalSql.length());
    result.setSystemTimesForTables(systemTimesForTables);
    result.setModifiedSql(sqlWithoutSystemTime.toString());
  }

  private String currentDateTime() {
    return LOCAL_DATE_TIME.format(LocalDateTime.now());
  }

  static String cutGroup(String before, Matcher matcher, int group) {
    final int start = matcher.start(group);
    return start == -1 ? before : before.substring(0, start) + before.substring(matcher.end(group));
  }
}

@FunctionalInterface
interface SqlModifier {
  void modify(Matcher matcher, RegexPreprocessorResult result);
}

class PatternModifierPair {
  Pattern regexPattern;
  SqlModifier sqlModifier;

  PatternModifierPair(Pattern regexPattern, SqlModifier sqlModifier) {
    this.regexPattern = regexPattern;
    this.sqlModifier = sqlModifier;
  }
}
