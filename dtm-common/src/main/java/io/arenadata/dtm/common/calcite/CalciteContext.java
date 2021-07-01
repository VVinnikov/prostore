package io.arenadata.dtm.common.calcite;

import lombok.Data;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelBuilder;

@Data
public class CalciteContext {
  private final SchemaPlus schema;
  private final Planner planner;
  private final RelBuilder relBuilder;
}
