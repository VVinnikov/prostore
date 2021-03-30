package io.arenadata.dtm.common.cache;

import lombok.Builder;
import lombok.Data;
import org.apache.calcite.sql.SqlNode;

@Data
@Builder
public class QueryTemplateValue {
    private SqlNode enrichQueryTemplateNode;
}
