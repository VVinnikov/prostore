package io.arenadata.dtm.common.cache;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.calcite.sql.SqlNode;

@Data
@AllArgsConstructor
public class PreparedQueryValue {
    private final SqlNode sqlNode;
}
