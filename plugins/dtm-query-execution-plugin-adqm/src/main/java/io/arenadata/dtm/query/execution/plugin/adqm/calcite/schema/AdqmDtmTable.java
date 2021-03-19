package io.arenadata.dtm.query.execution.plugin.adqm.calcite.schema;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.calcite.core.schema.DtmTable;
import io.arenadata.dtm.query.calcite.core.schema.QueryableSchema;

public class AdqmDtmTable extends DtmTable {
    public AdqmDtmTable(QueryableSchema dtmSchema, Entity entity) {
        super(dtmSchema, entity);
    }
}
