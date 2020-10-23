package io.arenadata.dtm.query.execution.plugin.adg.dto.schema;


import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

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
