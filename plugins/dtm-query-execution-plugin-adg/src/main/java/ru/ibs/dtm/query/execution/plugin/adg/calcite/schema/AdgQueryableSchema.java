package ru.ibs.dtm.query.execution.plugin.adg.calcite.schema;

import org.apache.calcite.schema.impl.AbstractSchema;
import ru.ibs.dtm.query.execution.plugin.adg.calcite.schema.dialect.AdgDtmConvention;

public class AdgQueryableSchema extends AbstractSchema {

    private AdgDtmConvention convention;

    public AdgQueryableSchema(AdgDtmConvention convention) {
        this.convention = convention;
    }

    public AdgDtmConvention getConvention() {
        return convention;
    }
}
