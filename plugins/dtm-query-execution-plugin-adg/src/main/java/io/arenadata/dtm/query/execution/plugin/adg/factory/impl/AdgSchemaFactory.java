package io.arenadata.dtm.query.execution.plugin.adg.factory.impl;

import io.arenadata.dtm.query.calcite.core.factory.impl.DtmSchemaFactory;
import io.arenadata.dtm.query.calcite.core.schema.dialect.DtmConvention;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adg.calcite.schema.dialect.AdgDtmConvention;
import org.apache.calcite.linq4j.tree.Expression;
import org.springframework.stereotype.Service;

@Service("adgSchemaFactory")
public class AdgSchemaFactory extends DtmSchemaFactory {

    @Override
    protected DtmConvention createDtmConvention(Datamart datamart, Expression expression) {
        return new AdgDtmConvention(datamart, expression);
    }
}
