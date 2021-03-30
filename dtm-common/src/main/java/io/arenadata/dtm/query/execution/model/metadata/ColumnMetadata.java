package io.arenadata.dtm.query.execution.model.metadata;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.SystemMetadata;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ColumnMetadata {
    /**
     * Column name
     */
    private String name;
    /**
     * System column type
     */
    private SystemMetadata systemMetadata;
    /**
     * Column data type
     */
    private ColumnType type;

    /**
     * Column size
     */
    private Integer size;

    public ColumnMetadata(String name, ColumnType type) {
        this.name = name;
        this.type = type;
    }

    public ColumnMetadata(String name, SystemMetadata systemMetadata, ColumnType type) {
        this.name = name;
        this.systemMetadata = systemMetadata;
        this.type = type;
    }

    public ColumnMetadata(String name, ColumnType type, Integer size) {
        this.name = name;
        this.type = type;
        this.size = size;
    }
}
