package io.arenadata.dtm.query.execution.plugin.adqm.calcite.model.schema.dialect;

import io.arenadata.dtm.query.calcite.core.schema.dialect.DtmConvention;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import org.apache.calcite.linq4j.tree.Expression;

/**
 * Кастомизированный Relation Trait
 */
public class AdqmDtmConvention extends DtmConvention {
    public AdqmDtmConvention(Datamart datamart, Expression schemaExpression) {
        super(datamart, schemaExpression);
    }
}
