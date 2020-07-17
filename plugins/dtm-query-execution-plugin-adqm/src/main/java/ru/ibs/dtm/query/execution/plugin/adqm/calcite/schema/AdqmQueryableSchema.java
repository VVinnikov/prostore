package ru.ibs.dtm.query.execution.plugin.adqm.calcite.schema;

import org.apache.calcite.schema.impl.AbstractSchema;
import ru.ibs.dtm.query.execution.plugin.adqm.calcite.schema.dialect.AdqmDtmConvention;

public class AdqmQueryableSchema extends AbstractSchema {

    private final AdqmDtmConvention convention;

    public AdqmQueryableSchema(AdqmDtmConvention convention) {
        this.convention = convention;
    }

    public AdqmDtmConvention getConvention() {
        return convention;
    }
}
