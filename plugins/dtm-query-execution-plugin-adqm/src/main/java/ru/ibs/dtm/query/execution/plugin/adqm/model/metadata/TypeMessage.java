package ru.ibs.dtm.query.execution.plugin.adqm.model.metadata;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TypeMessage {
    private UUID id;
    /**
     * Тип данных в колонке
     */
    private ColumnType value;
}
