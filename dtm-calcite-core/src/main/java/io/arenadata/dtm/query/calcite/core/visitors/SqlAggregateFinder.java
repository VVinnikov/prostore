package io.arenadata.dtm.query.calcite.core.visitors;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.util.SqlBasicVisitor;

public class SqlAggregateFinder extends SqlBasicVisitor<Object> {
    private boolean foundAggregate;

    @Override
    public Object visit(SqlCall call) {
        if (call.getOperator().isAggregator() || call.getKind() == SqlKind.OTHER_FUNCTION) {
            foundAggregate = true;
            return null;
        }

        return super.visit(call);
    }

    public boolean isFoundAggregate() {
        return foundAggregate;
    }
}
