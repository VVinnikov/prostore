package io.arenadata.dtm.query.execution.plugin.adb.calcite.model.schema;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.calcite.core.schema.DtmTable;
import io.arenadata.dtm.query.calcite.core.schema.QueryableSchema;

public class AdbDtmTable extends DtmTable {
    public AdbDtmTable(QueryableSchema dtmSchema, Entity entity) {
        super(dtmSchema, entity);
    }
}
