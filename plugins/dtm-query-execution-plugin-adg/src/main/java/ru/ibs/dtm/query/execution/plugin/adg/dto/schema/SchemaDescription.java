package ru.ibs.dtm.query.execution.plugin.adg.dto.schema;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;

/**
 * Описание схемы
 * */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SchemaDescription {
  private Datamart logicalSchema;
  private Datamart physicalSchema;
}
