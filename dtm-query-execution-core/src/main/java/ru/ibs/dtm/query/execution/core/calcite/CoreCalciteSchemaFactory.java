package ru.ibs.dtm.query.execution.core.calcite;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.calcite.core.factory.SchemaFactory;
import ru.ibs.dtm.query.calcite.core.factory.impl.CalciteSchemaFactory;
import ru.ibs.dtm.query.calcite.core.schema.DtmTable;
import ru.ibs.dtm.query.calcite.core.schema.QueryableSchema;
import ru.ibs.dtm.query.execution.core.calcite.schema.CoreDtmTable;
import ru.ibs.dtm.query.execution.model.metadata.DatamartTable;

@Component("coreCalciteSchemaFactory")
public class CoreCalciteSchemaFactory extends CalciteSchemaFactory {
    public CoreCalciteSchemaFactory(@Qualifier("coreSchemaFactory") SchemaFactory schemaFactory) {
        super(schemaFactory);
    }

    @Override
    protected DtmTable createTable(QueryableSchema schema, DatamartTable datamartTable) {
        return new CoreDtmTable(schema, datamartTable);
    }
}
