package ru.ibs.dtm.query.execution.core.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QuerySourceRequest;
import ru.ibs.dtm.common.reader.SourceType;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Determining the type of request by hint DATASOURCE_TYPE = 'ADB|ADG|ADQM'
 */
@Slf4j
@Component
public class HintExtractor {
    private final Pattern HINT_PATTERN = Pattern.compile(
        "DATASOURCE_TYPE\\s*=\\s*'([^\\s]+)'\\s*$",
        Pattern.CASE_INSENSITIVE);

    public QuerySourceRequest extractHint(QueryRequest request) {
        QuerySourceRequest sourceRequest = new QuerySourceRequest();
        Matcher matcher = HINT_PATTERN.matcher(request.getSql());
        if (matcher.find()) {
            String dataSource = matcher.group(1);
            String strippedSql = matcher.replaceFirst(StringUtils.EMPTY).trim();
            QueryRequest newQueryRequest = request.copy();
            newQueryRequest.setSql(strippedSql);
            try {
                sourceRequest.setSourceType(SourceType.valueOf(dataSource));
            }
            catch (IllegalArgumentException e)
            {
                throw new IllegalArgumentException("\"" + dataSource + "\""
                    + " isn't a valid datasource type, please use one of the following: " +
                    Arrays.stream(SourceType.values()).filter(type -> !type.equals(SourceType.INFORMATION_SCHEMA))
                            .map(SourceType::name).collect(Collectors.joining(", ")), e);
            }
            sourceRequest.setQueryRequest(newQueryRequest);
        } else {
            log.info("Hint not defined for request {}", request.getSql());
            sourceRequest.setQueryRequest(request);
        }
        return sourceRequest;
    }
}
