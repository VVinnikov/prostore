package ru.ibs.dtm.query.execution.model.metadata;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;

/**
 * Описание схемы SchemaDescription
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Datamart implements Serializable {
    private UUID id;
    /**
     * Имя схемы
     */
    private String mnemonic;
    /**
     * признак витрины по умолчанию
     */
    private Boolean isDefault = false;
    /**
     * Описание таблиц в схеме
     */
    private List<DatamartTable> datamartTables;
}



