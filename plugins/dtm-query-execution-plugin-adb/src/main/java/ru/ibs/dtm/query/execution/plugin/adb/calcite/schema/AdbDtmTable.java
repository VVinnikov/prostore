package ru.ibs.dtm.query.execution.plugin.adb.calcite.schema;

import ru.ibs.dtm.query.calcite.core.schema.DtmTable;
import ru.ibs.dtm.query.calcite.core.schema.QueryableSchema;
import ru.ibs.dtm.query.execution.model.metadata.DatamartTable;

public class AdbDtmTable extends DtmTable {
    public AdbDtmTable(QueryableSchema dtmSchema, DatamartTable datamartTable) {
        super(dtmSchema, datamartTable);
    }
}
