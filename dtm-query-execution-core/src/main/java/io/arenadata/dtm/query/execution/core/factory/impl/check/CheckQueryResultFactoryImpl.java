package io.arenadata.dtm.query.execution.core.factory.impl.check;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.factory.CheckQueryResultFactory;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Component
public class CheckQueryResultFactoryImpl implements CheckQueryResultFactory {

    private static final String CHECK_RESULT_COLUMN_NAME = "check_result";

    @Override
    public QueryResult create(String result) {
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put(CHECK_RESULT_COLUMN_NAME, result);
        return QueryResult.builder()
                //.requestId(requestId)
                .metadata(Collections.singletonList(ColumnMetadata.builder()
                        .name(CHECK_RESULT_COLUMN_NAME)
                        .type(ColumnType.VARCHAR)
                        .build()))
                .result(Collections.singletonList(resultMap))
                .build();
    }
}
