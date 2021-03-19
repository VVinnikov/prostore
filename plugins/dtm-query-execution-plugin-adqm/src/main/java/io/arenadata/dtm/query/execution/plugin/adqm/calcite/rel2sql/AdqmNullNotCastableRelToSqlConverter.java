package io.arenadata.dtm.query.execution.plugin.adqm.calcite.rel2sql;

import lombok.val;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.adapter.enumerable.EnumerableSort;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import java.util.ArrayList;
import java.util.List;

public class AdqmNullNotCastableRelToSqlConverter extends RelToSqlConverter {
    /**
     * Creates a RelToSqlConverter.
     *
     * @param dialect
     */
    public AdqmNullNotCastableRelToSqlConverter(SqlDialect dialect) {
        super(dialect);
    }

    @Override
    public Result visit(Project e) {
        e.getVariablesSet();
        Result x = visitChild(0, e.getInput());
        parseCorrelTable(e, x);
        final Builder builder = x.builder(e, Clause.SELECT);
        final List<SqlNode> selectList = new ArrayList<>();
        for (RexNode ref : e.getChildExps()) {
            SqlNode sqlExpr = builder.context.toSql(null, ref);
            addSelect(selectList, sqlExpr, e.getRowType());
        }

        builder.setSelect(new SqlNodeList(selectList, POS));
        return builder.result();
    }

    private void parseCorrelTable(RelNode relNode, Result x) {
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
                builder.setSelect(x.asSelect().getSelectList());
                builder.setFetch(SqlLiteral.createExactNumeric(((EnumerableLimit) e).fetch.toStringRaw(), POS));
                return builder.result();
            }
        } else {
            throw new AssertionError("Need to implement " + e.getClass().getName());
        }
    }



    @Override
    protected boolean isAnon() {
        return false;
    }
}
