package ru.ibs.dtm.query.execution.model.metadata;

import lombok.*;
import ru.ibs.dtm.common.model.ddl.ColumnType;
import ru.ibs.dtm.common.model.ddl.SystemMetadata;

@Data
@RequiredArgsConstructor
@AllArgsConstructor
@NoArgsConstructor
public class ColumnMetadata {
  /**
   * Название колонки
   */
  @NonNull
  private String name;
  /**
   * Тип системного столбца
   */
  private SystemMetadata systemMetadata;
  /**
   * Тип данных в колонке
   */
  @NonNull
  private ColumnType type;
}
