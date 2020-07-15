package ru.ibs.dtm.query.execution.plugin.adg.calcite.schema.dialect;

import org.apache.calcite.linq4j.tree.Expression;
import ru.ibs.dtm.query.calcite.core.schema.dialect.DtmConvention;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;

/**
 * Кастомизированный Relation Trait
 */
public class AdgDtmConvention extends DtmConvention {
    public AdgDtmConvention(Datamart datamart, Expression schemaExpression) {
        super(datamart, schemaExpression);
    }
}
