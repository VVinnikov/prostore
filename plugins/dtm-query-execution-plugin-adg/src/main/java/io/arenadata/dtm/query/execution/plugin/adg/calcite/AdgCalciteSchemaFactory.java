package io.arenadata.dtm.query.execution.plugin.adg.calcite;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.calcite.core.factory.SchemaFactory;
import io.arenadata.dtm.query.calcite.core.factory.impl.CalciteSchemaFactory;
import io.arenadata.dtm.query.calcite.core.schema.DtmTable;
import io.arenadata.dtm.query.calcite.core.schema.QueryableSchema;
import io.arenadata.dtm.query.execution.plugin.adg.calcite.schema.AdgDtmTable;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service("adgCalciteSchemaFactory")
public class AdgCalciteSchemaFactory extends CalciteSchemaFactory {

    public AdgCalciteSchemaFactory(@Qualifier("adgSchemaFactory") SchemaFactory schemaFactory) {
        super(schemaFactory);
    }

    @Override
    protected DtmTable createTable(QueryableSchema schema, Entity entity) {
        return new AdgDtmTable(schema, entity);
    }
}
