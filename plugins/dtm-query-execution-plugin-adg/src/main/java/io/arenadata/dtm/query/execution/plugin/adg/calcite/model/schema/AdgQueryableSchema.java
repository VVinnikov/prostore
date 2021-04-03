package io.arenadata.dtm.query.execution.plugin.adg.calcite.model.schema;

import io.arenadata.dtm.query.execution.plugin.adg.calcite.model.schema.dialect.AdgDtmConvention;
import org.apache.calcite.schema.impl.AbstractSchema;

public class AdgQueryableSchema extends AbstractSchema {

    private AdgDtmConvention convention;

    public AdgQueryableSchema(AdgDtmConvention convention) {
        this.convention = convention;
    }

    public AdgDtmConvention getConvention() {
        return convention;
    }
}
