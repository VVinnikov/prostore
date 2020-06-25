package ru.ibs.dtm.query.execution.core.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QuerySourceRequest;
import ru.ibs.dtm.common.reader.SourceType;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Определение типа запроса по хинту DATASOURCE_TYPE = ''
 */
@Slf4j
@Component
public class HintExtractor {
    private final Pattern HINT_PATTERN = Pattern.compile(
            "(.*)[\\s]+DATASOURCE_TYPE[\\s]*=[\\s]*'([^\\s]+)'",
            Pattern.CASE_INSENSITIVE);

    public QuerySourceRequest extractHint(QueryRequest request) {
        QuerySourceRequest sourceRequest = new QuerySourceRequest();
        Matcher matcher = HINT_PATTERN.matcher(request.getSql());
        if (matcher.find()) {
            String newSql = matcher.group(1);
            String dataSource = matcher.group(2);
            QueryRequest newQueryRequest = request.copy();
            newQueryRequest.setSql(newSql);
            sourceRequest.setSourceType(SourceType.valueOf(dataSource));
            sourceRequest.setQueryRequest(newQueryRequest);
        } else {
            log.info("Не определен хинт для запроса {}", request.getSql());
            sourceRequest.setQueryRequest(request);
        }
        return sourceRequest;
    }
}
