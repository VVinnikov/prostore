package ru.ibs.dtm.query.execution.core.calcite.schema;

import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.query.calcite.core.schema.DtmTable;
import ru.ibs.dtm.query.calcite.core.schema.QueryableSchema;

public class CoreDtmTable extends DtmTable {
    public CoreDtmTable(QueryableSchema dtmSchema, Entity entity) {
        super(dtmSchema, entity);
    }
}
