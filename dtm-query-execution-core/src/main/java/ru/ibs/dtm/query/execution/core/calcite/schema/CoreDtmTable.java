package ru.ibs.dtm.query.execution.core.calcite.schema;

import ru.ibs.dtm.query.calcite.core.schema.DtmTable;
import ru.ibs.dtm.query.calcite.core.schema.QueryableSchema;
import ru.ibs.dtm.query.execution.model.metadata.DatamartTable;

public class CoreDtmTable extends DtmTable {
    public CoreDtmTable(QueryableSchema dtmSchema, DatamartTable datamartTable) {
        super(dtmSchema, datamartTable);
    }
}
