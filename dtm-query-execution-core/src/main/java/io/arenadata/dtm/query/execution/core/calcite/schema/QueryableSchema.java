package io.arenadata.dtm.query.execution.core.calcite.schema;

import io.arenadata.dtm.query.execution.core.calcite.schema.dialect.CoreDtmConvention;
import org.apache.calcite.schema.impl.AbstractSchema;

public class QueryableSchema extends AbstractSchema {

  private CoreDtmConvention convention;

  public QueryableSchema(CoreDtmConvention convention) {
    this.convention = convention;
  }

  public CoreDtmConvention getConvention() {
    return convention;
  }
}
