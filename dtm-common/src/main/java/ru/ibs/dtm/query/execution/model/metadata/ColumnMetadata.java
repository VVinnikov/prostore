package ru.ibs.dtm.query.execution.model.metadata;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.ibs.dtm.common.model.ddl.ColumnType;

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
