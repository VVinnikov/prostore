package io.arenadata.dtm.query.execution.plugin.adp.calcite.factory;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.calcite.core.factory.SchemaFactory;
import io.arenadata.dtm.query.calcite.core.factory.impl.CalciteSchemaFactory;
import io.arenadata.dtm.query.calcite.core.schema.DtmTable;
import io.arenadata.dtm.query.calcite.core.schema.QueryableSchema;
import io.arenadata.dtm.query.execution.plugin.adp.calcite.model.schema.AdpDtmTable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service("adpCalciteSchemaFactory")
public class AdpCalciteSchemaFactory extends CalciteSchemaFactory {

    @Autowired
    public AdpCalciteSchemaFactory(@Qualifier("adpSchemaFactory") SchemaFactory schemaFactory) {
        super(schemaFactory);
    }

    @Override
    protected DtmTable createTable(QueryableSchema schema, Entity entity) {
        return new AdpDtmTable(schema, entity);
    }
}
