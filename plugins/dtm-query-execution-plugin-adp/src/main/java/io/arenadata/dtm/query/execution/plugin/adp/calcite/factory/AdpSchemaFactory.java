package io.arenadata.dtm.query.execution.plugin.adp.calcite.factory;

import io.arenadata.dtm.query.calcite.core.factory.impl.DtmSchemaFactory;
import io.arenadata.dtm.query.calcite.core.schema.dialect.DtmConvention;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adp.calcite.model.schema.dialect.AdpDtmConvention;
import org.apache.calcite.linq4j.tree.Expression;
import org.springframework.stereotype.Service;

@Service("adpSchemaFactory")
public class AdpSchemaFactory extends DtmSchemaFactory {
    @Override
    protected DtmConvention createDtmConvention(Datamart datamart, Expression expression) {
        return new AdpDtmConvention(datamart, expression);
    }
}
