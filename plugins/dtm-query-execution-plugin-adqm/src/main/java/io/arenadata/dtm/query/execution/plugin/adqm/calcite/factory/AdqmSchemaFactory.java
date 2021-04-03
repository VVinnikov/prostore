package io.arenadata.dtm.query.execution.plugin.adqm.calcite.factory;

import io.arenadata.dtm.query.calcite.core.factory.impl.DtmSchemaFactory;
import io.arenadata.dtm.query.calcite.core.schema.dialect.DtmConvention;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adqm.calcite.model.schema.dialect.AdqmDtmConvention;
import org.apache.calcite.linq4j.tree.Expression;
import org.springframework.stereotype.Service;

@Service("adqmSchemaFactory")
public class AdqmSchemaFactory extends DtmSchemaFactory {

    @Override
    protected DtmConvention createDtmConvention(Datamart datamart, Expression expression) {
        return new AdqmDtmConvention(datamart, expression);
    }
}
