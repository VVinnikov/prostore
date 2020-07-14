package ru.ibs.dtm.query.execution.plugin.adb.calcite;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.calcite.core.factory.SchemaFactory;
import ru.ibs.dtm.query.calcite.core.factory.impl.CalciteSchemaFactory;
import ru.ibs.dtm.query.calcite.core.schema.DtmTable;
import ru.ibs.dtm.query.calcite.core.schema.QueryableSchema;
import ru.ibs.dtm.query.execution.model.metadata.DatamartClass;
import ru.ibs.dtm.query.execution.plugin.adb.calcite.schema.AdbDtmTable;

@Service("adbCalciteSchemaFactory")
public class AdbCalciteSchemaFactory extends CalciteSchemaFactory {
    public AdbCalciteSchemaFactory(@Qualifier("adbSchemaFactory") SchemaFactory schemaFactory) {
        super(schemaFactory);
    }

    @Override
    protected DtmTable createTable(QueryableSchema schema, DatamartClass datamartClass) {
        return new AdbDtmTable(schema, datamartClass);
    }
}
