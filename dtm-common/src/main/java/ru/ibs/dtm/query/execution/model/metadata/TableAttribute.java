package ru.ibs.dtm.query.execution.model.metadata;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TableAttribute {
  private UUID id;
  /*Имя атрибута*/
  private String mnemonic;
  /*Тип атрибута*/
  private AttributeType type;
}
