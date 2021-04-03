package io.arenadata.dtm.query.execution.plugin.adg.base.dto.schema;


import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Schema description
 * */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SchemaDescription {
  private Datamart logicalSchema;
  private Datamart physicalSchema;
}
