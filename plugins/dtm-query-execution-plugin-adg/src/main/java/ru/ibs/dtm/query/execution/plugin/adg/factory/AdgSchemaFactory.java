package ru.ibs.dtm.query.execution.plugin.adg.factory;

import org.apache.calcite.linq4j.tree.Expression;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.calcite.core.factory.impl.DtmSchemaFactory;
import ru.ibs.dtm.query.calcite.core.schema.dialect.DtmConvention;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;
import ru.ibs.dtm.query.execution.plugin.adg.calcite.schema.dialect.AdgDtmConvention;

@Service("adgSchemaFactory")
public class AdgSchemaFactory extends DtmSchemaFactory {

    @Override
    protected DtmConvention createDtmConvention(Datamart datamart, Expression expression) {
        return new AdgDtmConvention(datamart, expression);
    }
}