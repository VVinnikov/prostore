package io.arenadata.dtm.query.execution.plugin.adp.calcite.model.schema;

import io.arenadata.dtm.query.execution.plugin.adp.calcite.model.schema.dialect.AdpDtmConvention;
import org.apache.calcite.schema.impl.AbstractSchema;

public class QueryableSchema extends AbstractSchema {

  private final AdpDtmConvention convention;

  public QueryableSchema(AdpDtmConvention convention) {
    this.convention = convention;
  }

  public AdpDtmConvention getConvention() {
    return convention;
  }
}
