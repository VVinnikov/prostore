package ru.ibs.dtm.query.execution.model.metadata;

import java.io.Serializable;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

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
