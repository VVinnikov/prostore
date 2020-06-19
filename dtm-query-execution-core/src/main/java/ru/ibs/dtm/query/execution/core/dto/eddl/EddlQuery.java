package ru.ibs.dtm.query.execution.core.dto.eddl;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import javax.validation.constraints.NotNull;

/**
 * Запрос eddl
 */
@Data
public abstract class EddlQuery {

    /**
     * Тип запроса
     */
    @NotNull
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
