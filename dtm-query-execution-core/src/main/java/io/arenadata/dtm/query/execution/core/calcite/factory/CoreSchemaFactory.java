package io.arenadata.dtm.query.execution.core.calcite.factory;

import io.arenadata.dtm.query.calcite.core.factory.impl.DtmSchemaFactory;
import io.arenadata.dtm.query.calcite.core.schema.dialect.DtmConvention;
import io.arenadata.dtm.query.execution.core.calcite.model.schema.dialect.CoreDtmConvention;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import org.apache.calcite.linq4j.tree.Expression;
import org.springframework.stereotype.Component;

@Component("coreSchemaFactory")
public class CoreSchemaFactory extends DtmSchemaFactory {
    @Override
    protected DtmConvention createDtmConvention(Datamart datamart, Expression expression) {
        return new CoreDtmConvention(datamart, expression);
    }
}
