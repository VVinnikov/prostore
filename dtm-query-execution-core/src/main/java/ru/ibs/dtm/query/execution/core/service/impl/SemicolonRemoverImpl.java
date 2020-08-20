package ru.ibs.dtm.query.execution.core.service.impl;

import lombok.val;
import org.apache.logging.log4j.util.Strings;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.core.service.SemicolonRemover;

@Component
public class SemicolonRemoverImpl implements SemicolonRemover {
    private static final String SEMICOLON_PATTERN = ";\\z";

    @Override
    public QueryRequest remove(QueryRequest queryRequest) {
        val withoutSemicolon = queryRequest.copy();
        withoutSemicolon.setSql(removeSemicolon(queryRequest));
        return withoutSemicolon;
    }

    private String removeSemicolon(QueryRequest queryRequest) {
        return queryRequest.getSql().trim().replaceAll(SEMICOLON_PATTERN, Strings.EMPTY);
    }
}
