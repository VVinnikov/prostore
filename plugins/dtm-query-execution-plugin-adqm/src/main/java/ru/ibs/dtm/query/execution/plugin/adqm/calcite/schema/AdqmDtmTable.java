package ru.ibs.dtm.query.execution.plugin.adqm.calcite.schema;

import ru.ibs.dtm.query.calcite.core.schema.DtmTable;
import ru.ibs.dtm.query.calcite.core.schema.QueryableSchema;
import ru.ibs.dtm.query.execution.model.metadata.DatamartTable;

public class AdqmDtmTable extends DtmTable {
    public AdqmDtmTable(QueryableSchema dtmSchema, DatamartTable datamartClass) {
        super(dtmSchema, datamartClass);
    }
}
