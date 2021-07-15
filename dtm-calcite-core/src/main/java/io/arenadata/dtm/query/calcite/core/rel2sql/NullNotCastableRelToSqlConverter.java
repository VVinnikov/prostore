package io.arenadata.dtm.query.calcite.core.rel2sql;

import lombok.val;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.adapter.enumerable.EnumerableSort;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.ArrayList;
import java.util.List;

public class NullNotCastableRelToSqlConverter extends RelToSqlConverter {

    public NullNotCastableRelToSqlConverter(SqlDialect dialect) {
        super(dialect);
    }

    @Override
    public Result visit(Project e) {
        e.getVariablesSet();
        Result x = visitChild(0, e.getInput());
        parseCorrelTable(e, x);
        if (isStar(e.getChildExps(), e.getInput().getRowType(), e.getRowType())) {
            return x;
        }
        final Builder builder = x.builder(e, Clause.SELECT);
        final List<SqlNode> selectList = new ArrayList<>();
        for (RexNode ref : e.getChildExps()) {
            SqlNode sqlExpr = builder.context.toSql(null, ref);
            addSelect(selectList, sqlExpr, e.getRowType());
        }

        builder.setSelect(new SqlNodeList(selectList, POS));
        return builder.result();
    }

    protected void parseCorrelTable(RelNode relNode, Result x) {
        for (CorrelationId id : relNode.getVariablesSet()) {
            correlTableMap.put(id, x.qualifiedContext());
        }
    }

    @Override
    public Result visit(RelNode e) {
        if (e instanceof EnumerableLimit) {
            e.getVariablesSet();
            RelNode input = e.getInput(0);
            if (input instanceof EnumerableSort) {
                val node = (EnumerableSort) input;
                val sort = EnumerableSort.create(node.getInput(),
                    node.getCollation(),
                    ((EnumerableLimit) e).offset,
                    ((EnumerableLimit) e).fetch);
                return visitChild(0, sort);
            } else {
                Result x = visitChild(0, input);
                parseCorrelTable(e, x);
                final Builder builder = x.builder(input, Clause.FETCH);
                handleCountAggregation(x, builder);
                setOffset(builder, e);
                setFetch(builder, e);
                return builder.result();
            }
        } else {
            throw new AssertionError("Need to implement " + e.getClass().getName());
        }
    }

    private void handleCountAggregation(Result x, Builder builder) {
        SqlNodeList selectList = x.asSelect().getSelectList();
        boolean hasCount = hasCount(selectList);
        if (hasCount) {
            x.asSelect().setSelectList(null);
        }
        builder.setSelect(selectList);
    }

    private boolean hasCount(SqlNodeList expression) {
        CountAggregateFinder finder = new CountAggregateFinder();
        expression.accept(finder);
        return finder.hasCount;
    }

    private void setOffset(Builder builder, RelNode node) {
        RexNode offset = ((EnumerableLimit) node).offset;
        if (offset != null) {
            builder.setOffset(SqlLiteral.createExactNumeric(offset.toStringRaw(), POS));
        }
    }

    private void setFetch(Builder builder, RelNode node) {
        RexNode fetch = ((EnumerableLimit) node).fetch;
        if (fetch != null) {
            builder.setFetch(SqlLiteral.createExactNumeric(fetch.toStringRaw(), POS));
        }
    }

    @Override
    protected boolean isAnon() {
        return false;
    }

    private class CountAggregateFinder extends SqlShuttle {
        private boolean hasCount;

        @Override
        public SqlNode visit(SqlCall call) {
            if (call.getOperator().getKind() == SqlKind.COUNT) {
                hasCount = true;
            }
            return super.visit(call);
        }

        public boolean hasCount() {
            return hasCount;
        }
    }
}
