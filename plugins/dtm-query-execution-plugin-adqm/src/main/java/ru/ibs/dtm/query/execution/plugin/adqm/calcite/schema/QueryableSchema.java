package ru.ibs.dtm.query.execution.plugin.adqm.calcite.schema;

import org.apache.calcite.schema.impl.AbstractSchema;
import ru.ibs.dtm.query.execution.plugin.adqm.calcite.schema.dialect.DtmConvention;

public class QueryableSchema extends AbstractSchema {

    private DtmConvention convention;

    public QueryableSchema(DtmConvention convention) {
        this.convention = convention;
    }

    public DtmConvention getConvention() {
        return convention;
    }
}
