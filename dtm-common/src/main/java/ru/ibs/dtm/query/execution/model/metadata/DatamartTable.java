package ru.ibs.dtm.query.execution.model.metadata;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Описание схемы таблицы
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DatamartTable implements Serializable {
    private UUID id;
    /**
     * Имя таблицы
     */
    private String mnemonic;
    /**
     * Имя схемы
     */
    private String datamartMnemonic;
    /**
     * Label таблицы
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
