package ru.ibs.dtm.query.calcite.core.rel2sql;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import java.util.ArrayList;
import java.util.List;

public class NullNotCastableRelToSqlConverter extends RelToSqlConverter {
    /**
     * Creates a RelToSqlConverter.
     *
     * @param dialect
     */
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

    private void parseCorrelTable(RelNode relNode, Result x) {
        for (CorrelationId id : relNode.getVariablesSet()) {
            correlTableMap.put(id, x.qualifiedContext());
        }
    }

    @Override
    protected boolean isAnon() {
        return false;
    }
}
