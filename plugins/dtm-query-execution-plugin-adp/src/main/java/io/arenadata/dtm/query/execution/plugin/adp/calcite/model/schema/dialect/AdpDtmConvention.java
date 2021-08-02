package io.arenadata.dtm.query.execution.plugin.adp.calcite.model.schema.dialect;

import io.arenadata.dtm.query.calcite.core.schema.dialect.DtmConvention;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import org.apache.calcite.linq4j.tree.Expression;

public class AdpDtmConvention extends DtmConvention {
    public AdpDtmConvention(Datamart datamart, Expression schemaExpression) {
        super(datamart, schemaExpression);
    }
}
