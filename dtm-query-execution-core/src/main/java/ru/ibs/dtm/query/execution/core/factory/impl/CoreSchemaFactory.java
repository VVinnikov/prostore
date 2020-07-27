package ru.ibs.dtm.query.execution.core.factory.impl;

import org.apache.calcite.linq4j.tree.Expression;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.calcite.core.factory.impl.DtmSchemaFactory;
import ru.ibs.dtm.query.calcite.core.schema.dialect.DtmConvention;
import ru.ibs.dtm.query.execution.core.calcite.schema.dialect.CoreDtmConvention;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;

@Component("coreSchemaFactory")
public class CoreSchemaFactory extends DtmSchemaFactory {
    @Override
    protected DtmConvention createDtmConvention(Datamart datamart, Expression expression) {
        return new CoreDtmConvention(datamart, expression);
    }
}
