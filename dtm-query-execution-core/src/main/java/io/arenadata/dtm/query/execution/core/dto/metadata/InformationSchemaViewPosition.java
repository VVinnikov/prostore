package io.arenadata.dtm.query.execution.core.dto.metadata;

import io.arenadata.dtm.common.reader.InformationSchemaView;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

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
