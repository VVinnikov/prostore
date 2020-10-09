package ru.ibs.dtm.query.execution.plugin.adb.calcite.schema;

import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.query.calcite.core.schema.DtmTable;
import ru.ibs.dtm.query.calcite.core.schema.QueryableSchema;

public class AdbDtmTable extends DtmTable {
    public AdbDtmTable(QueryableSchema dtmSchema, Entity entity) {
        super(dtmSchema, entity);
    }
}
