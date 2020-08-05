package ru.ibs.dtm.query.execution.plugin.adb.service.impl.query;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.plugin.adb.dto.RegexPreprocessorResult;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.temporal.ChronoField.*;

/**
 * Парсер для извлечения параметра SelectOn
 */
@Service
public class QueryRegexPreprocessor {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryRegexPreprocessor.class);

  static final DateTimeFormatter LOCAL_DATE_TIME = new DateTimeFormatterBuilder()
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

  static final Pattern SELECT_FOR_SYSTEM_TIME = Pattern.compile(
    // select * from test_datamart.pso FOR SYSTEM_TIME AS OF TIMESTAMP '1999-01-08 04:05:06';
    "\\s*select\\s+(.*)\\s+from\\s+([A-z.0-9\"]+)(\\s+FOR\\s+SYSTEM_TIME\\s+AS\\s+OF\\s+(TIMESTAMP\\s+)?'([0-9\\-]+\\s+[0-9:]+)')?(\\s+where\\s(.*))?.*",
    Pattern.CASE_INSENSITIVE
  );

  private PatternModifierPair[] patternModifierPairs;

  public QueryRegexPreprocessor() {
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
    final String dateTime = matcher.group(5);
    result.setSystemTimeAsOf(dateTime != null ? dateTime : LOCAL_DATE_TIME.format(LocalDateTime.now()));
    result.setModifiedSql(cutGroup(result.getModifiedSql(), matcher, 3));
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
