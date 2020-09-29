package ru.ibs.dtm.query.execution.plugin.adqm.calcite;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.query.calcite.core.factory.SchemaFactory;
import ru.ibs.dtm.query.calcite.core.factory.impl.CalciteSchemaFactory;
import ru.ibs.dtm.query.calcite.core.schema.DtmTable;
import ru.ibs.dtm.query.calcite.core.schema.QueryableSchema;
import ru.ibs.dtm.query.execution.plugin.adqm.calcite.schema.AdqmDtmTable;

@Service("adqmCalciteSchemaFactory")
public class AdqmCalciteSchemaFactory extends CalciteSchemaFactory {

    public AdqmCalciteSchemaFactory(@Qualifier("adqmSchemaFactory") SchemaFactory schemaFactory) {
        super(schemaFactory);
    }

    @Override
    protected DtmTable createTable(QueryableSchema schema, Entity entity) {
        return new AdqmDtmTable(schema, entity);
    }
}
