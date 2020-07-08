package ru.ibs.dtm.query.execution.plugin.adb.calcite.schema;

import org.apache.calcite.schema.impl.AbstractSchema;
import ru.ibs.dtm.query.execution.plugin.adb.calcite.schema.dialect.AdbDtmConvention;

public class QueryableSchema extends AbstractSchema {

  private AdbDtmConvention convention;

  public QueryableSchema(AdbDtmConvention convention) {
    this.convention = convention;
  }

  public AdbDtmConvention getConvention() {
    return convention;
  }
}
