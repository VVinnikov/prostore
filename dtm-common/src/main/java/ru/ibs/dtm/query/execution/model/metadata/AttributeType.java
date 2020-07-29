package ru.ibs.dtm.query.execution.model.metadata;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.ibs.dtm.common.model.ddl.ColumnType;

import java.io.Serializable;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AttributeType implements Serializable {
    private UUID id;
    /**
     * Тип данных в колонке
     */
    private ColumnType value;
}
