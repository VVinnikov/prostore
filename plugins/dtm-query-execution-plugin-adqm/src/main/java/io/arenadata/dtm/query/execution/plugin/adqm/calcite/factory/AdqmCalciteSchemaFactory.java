package io.arenadata.dtm.query.execution.plugin.adqm.calcite.factory;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.calcite.core.factory.SchemaFactory;
import io.arenadata.dtm.query.calcite.core.factory.impl.CalciteSchemaFactory;
import io.arenadata.dtm.query.calcite.core.schema.DtmTable;
import io.arenadata.dtm.query.calcite.core.schema.QueryableSchema;
import io.arenadata.dtm.query.execution.plugin.adqm.calcite.model.schema.AdqmDtmTable;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

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
