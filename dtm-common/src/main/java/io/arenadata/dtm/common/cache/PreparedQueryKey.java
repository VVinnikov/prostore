package io.arenadata.dtm.common.cache;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class PreparedQueryKey {
    private final String preparedQuerySql;
}
