package io.arenadata.dtm.common.reader;

import lombok.Data;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

@Data
public class QueryTemplateResult {
    private final String template;
    private final SqlNode templateNode;
    private final List<SqlNode> params;
}
