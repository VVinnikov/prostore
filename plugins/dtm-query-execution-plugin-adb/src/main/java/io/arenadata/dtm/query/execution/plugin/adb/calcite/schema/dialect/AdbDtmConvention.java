package io.arenadata.dtm.query.execution.plugin.adb.calcite.schema.dialect;

import io.arenadata.dtm.query.calcite.core.schema.dialect.DtmConvention;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import org.apache.calcite.linq4j.tree.Expression;

/**
 * Кастомизированный Relation Trait
 */
public class AdbDtmConvention extends DtmConvention {
    public AdbDtmConvention(Datamart datamart, Expression schemaExpression) {
        super(datamart, schemaExpression);
    }
}
