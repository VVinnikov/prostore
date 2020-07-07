package ru.ibs.dtm.query.execution.core.dto.metadata;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.ibs.dtm.common.reader.InformationSchemaView;

/**
 * Положение представления информационной схемы в запросе
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class InformationSchemaViewPosition {

  /**
   * Представление
   */
  private InformationSchemaView view;

  /**
   * Начало
   */
  private int start;

  /**
   * Конец
   */
  private int end;
}
