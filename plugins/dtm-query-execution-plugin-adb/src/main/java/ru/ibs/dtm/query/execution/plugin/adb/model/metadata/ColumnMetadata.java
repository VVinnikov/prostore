package ru.ibs.dtm.query.execution.plugin.adb.model.metadata;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ColumnMetadata {
  /**
   * Название колонки
   */
  private String name;
  /**
   * Тип данных в колонке
   */
  private ColumnType type;
}
