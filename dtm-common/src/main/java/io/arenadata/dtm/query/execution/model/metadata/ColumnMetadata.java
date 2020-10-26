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
     * Название колонки
     */
    private String name;
    /**
     * Тип системного столбца
     */
    private SystemMetadata systemMetadata;
    /**
     * Тип данных в колонке
     */
    private ColumnType type;

    public ColumnMetadata(String name, ColumnType type) {
        this.name = name;
        this.type = type;
    }
}
