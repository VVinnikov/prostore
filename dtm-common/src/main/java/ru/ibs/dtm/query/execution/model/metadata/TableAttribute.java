package ru.ibs.dtm.query.execution.model.metadata;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TableAttribute {
  /**
   * Uuid
   */
  private UUID id;
  /**
   * Имя атрибута
   */
  private String mnemonic;
  /**
   * Тип атрибута
   */
  private AttributeType type;
  /**
   * Длина атрибута
   */
  private Integer length;
  /**
   * Размерность атрибута
   */
  private Integer accuracy;
  /**
   * Порядковый номер первичного ключа
   */
  private Integer primaryKeyOrder;
  /**
   * Порядковый номер distribute ключа
   */
  private Integer distributeKeyOrder;
}
