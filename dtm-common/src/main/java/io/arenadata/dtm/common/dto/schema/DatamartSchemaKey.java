package io.arenadata.dtm.common.dto.schema;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class DatamartSchemaKey {
    private String schema;
    private String table;
}
