package io.arenadata.dtm.query.execution.core.dto.eddl;

import lombok.Data;

/**
 * Запрос eddl
 */
@Data
public abstract class EddlQuery {

    /**
     * Тип запроса
     */
    private EddlAction action;

    /**
     * Наименование схемы
     */
    private String schemaName;

    /**
     * Наименование таблицы
     */
    private String tableName;

    public EddlQuery(EddlAction action) {
        this.action = action;
    }

    public EddlQuery(EddlAction action, String schemaName, String tableName) {
        this(action);
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

}
