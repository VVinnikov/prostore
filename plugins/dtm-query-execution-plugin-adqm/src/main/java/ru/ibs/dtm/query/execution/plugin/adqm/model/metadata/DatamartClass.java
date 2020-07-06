package ru.ibs.dtm.query.execution.plugin.adqm.model.metadata;

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
public class DatamartClass {
    private UUID id;
    /*Имя схемы*/
    private String mnemonic;
    private String label;
    /*Атрибуты таблиц*/
    private List<ClassAttribute> classAttributes;
    private List<ClassAttribute> primaryKey;
}
