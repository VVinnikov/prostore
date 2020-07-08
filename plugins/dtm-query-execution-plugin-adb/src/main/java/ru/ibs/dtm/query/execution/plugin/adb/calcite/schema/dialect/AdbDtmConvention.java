package ru.ibs.dtm.query.execution.plugin.adb.calcite.schema.dialect;

import org.apache.calcite.linq4j.tree.Expression;
import ru.ibs.dtm.query.calcite.core.schema.dialect.DtmConvention;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;

/**
 * Кастомизированный Relation Trait
 */
public class AdbDtmConvention extends DtmConvention {
    public AdbDtmConvention(Datamart datamart, Expression schemaExpression) {
        super(datamart, schemaExpression);
    }
}
