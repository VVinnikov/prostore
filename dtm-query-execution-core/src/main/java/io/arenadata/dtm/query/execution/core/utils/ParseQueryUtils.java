package io.arenadata.dtm.query.execution.core.utils;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;

@Component
public class ParseQueryUtils {
    public String getDatamartName(List<SqlNode> operandList) {
        return operandList.stream()
                .filter(t -> t instanceof SqlIdentifier)
                .findFirst()
                .map(Objects::toString)
                .orElseThrow(() -> new RuntimeException("Can't get datamart name."));
    }
}