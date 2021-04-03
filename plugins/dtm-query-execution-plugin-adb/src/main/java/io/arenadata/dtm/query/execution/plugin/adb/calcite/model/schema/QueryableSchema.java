package io.arenadata.dtm.query.execution.plugin.adb.calcite.model.schema;

import io.arenadata.dtm.query.execution.plugin.adb.calcite.model.schema.dialect.AdbDtmConvention;
import org.apache.calcite.schema.impl.AbstractSchema;

public class QueryableSchema extends AbstractSchema {

  private final AdbDtmConvention convention;

  public QueryableSchema(AdbDtmConvention convention) {
    this.convention = convention;
  }

  public AdbDtmConvention getConvention() {
    return convention;
  }
}
