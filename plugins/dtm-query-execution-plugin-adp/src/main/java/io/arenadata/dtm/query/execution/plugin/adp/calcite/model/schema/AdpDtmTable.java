package io.arenadata.dtm.query.execution.plugin.adp.calcite.model.schema;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.calcite.core.schema.DtmTable;
import io.arenadata.dtm.query.calcite.core.schema.QueryableSchema;

public class AdpDtmTable extends DtmTable {
    public AdpDtmTable(QueryableSchema dtmSchema, Entity entity) {
        super(dtmSchema, entity);
    }
}
