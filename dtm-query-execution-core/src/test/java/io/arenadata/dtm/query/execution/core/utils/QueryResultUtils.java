package io.arenadata.dtm.query.execution.core.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class QueryResultUtils {

    public static List<Map<String, Object>> createResultWithSingleRow(List<String> columns, List<Object> values){
        List<Map<String, Object>> result = new ArrayList<>();

        assert !columns.isEmpty() || !values.isEmpty();
        assert columns.size() == values.size();

        Map<String, Object> rowMap = new HashMap<>();
        IntStream.range(0, columns.size()).forEach(i -> {
            rowMap.put(columns.get(i), values.get(i));
        });
        result.add(rowMap);
        return result;
    }
}
