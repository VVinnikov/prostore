package ru.ibs.dtm.query.execution.core.calcite.schema;

import org.apache.calcite.schema.impl.AbstractSchema;
import ru.ibs.dtm.query.execution.core.calcite.schema.dialect.CoreDtmConvention;

public class QueryableSchema extends AbstractSchema {

  private CoreDtmConvention convention;

  public QueryableSchema(CoreDtmConvention convention) {
    this.convention = convention;
  }

  public CoreDtmConvention getConvention() {
    return convention;
  }
}
