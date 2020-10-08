package ru.ibs.dtm.query.execution.model.metadata;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.ibs.dtm.common.model.ddl.ColumnType;
import ru.ibs.dtm.common.model.ddl.SystemMetadata;

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
