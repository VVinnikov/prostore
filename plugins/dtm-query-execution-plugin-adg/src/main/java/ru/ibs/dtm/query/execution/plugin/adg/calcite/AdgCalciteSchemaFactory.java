package ru.ibs.dtm.query.execution.plugin.adg.calcite;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.calcite.core.factory.SchemaFactory;
import ru.ibs.dtm.query.calcite.core.factory.impl.CalciteSchemaFactory;
import ru.ibs.dtm.query.calcite.core.schema.DtmTable;
import ru.ibs.dtm.query.calcite.core.schema.QueryableSchema;
import ru.ibs.dtm.query.execution.model.metadata.DatamartClass;
import ru.ibs.dtm.query.execution.plugin.adg.calcite.schema.AdgDtmTable;

@Service("adgCalciteSchemaFactory")
public class AdgCalciteSchemaFactory extends CalciteSchemaFactory {

    public AdgCalciteSchemaFactory(@Qualifier("adgSchemaFactory") SchemaFactory schemaFactory) {
        super(schemaFactory);
    }

    @Override
    protected DtmTable createTable(QueryableSchema schema, DatamartClass datamartClass) {
        return new AdgDtmTable(schema, datamartClass);
    }
}
