package ru.ibs.dtm.query.execution.model.metadata;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.UUID;

/**
 * Описание схемы таблицы
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DatamartTable {
    private UUID id;
    /**
     * Имя схемы
     */
    private String schema;
    /**
     * Имя таблицы
     */
    private String label;
    /**
     * Атрибуты таблиц
     */
    private List<TableAttribute> tableAttributes;
    /**
     * Первичные ключи таблицы
     */
    private List<TableAttribute> primaryKeys;
}
