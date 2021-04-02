package io.arenadata.dtm.query.execution.core.calcite.factory;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.calcite.core.factory.SchemaFactory;
import io.arenadata.dtm.query.calcite.core.factory.impl.CalciteSchemaFactory;
import io.arenadata.dtm.query.calcite.core.schema.DtmTable;
import io.arenadata.dtm.query.calcite.core.schema.QueryableSchema;
import io.arenadata.dtm.query.execution.core.calcite.model.schema.CoreDtmTable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component("coreCalciteSchemaFactory")
public class CoreCalciteSchemaFactory extends CalciteSchemaFactory {

    @Autowired
    public CoreCalciteSchemaFactory(@Qualifier("coreSchemaFactory") SchemaFactory schemaFactory) {
        super(schemaFactory);
    }

    @Override
    protected DtmTable createTable(QueryableSchema schema, Entity entity) {
        return new CoreDtmTable(schema, entity);
    }
}
